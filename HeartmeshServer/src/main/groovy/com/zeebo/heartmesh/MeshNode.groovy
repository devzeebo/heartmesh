package com.zeebo.heartmesh

import com.zeebo.heartmesh.util.ReaderCategory
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

class MeshNode {

	private int listenPort
	private Thread listenThread
	private ServerSocket socket

	private Map nodes

	private boolean isRunning() {
		listenThread != null && !listenThread.interrupted
	}

	MeshNode(int port) {
		listenPort = port
		socket = new ServerSocket(port)

		nodes = [:]
	}

	private boolean handleInput(InputStream stream) {
		def slurper = new JsonSlurper()
		def reader = stream.newReader()
		try {
			use(ReaderCategory) {
				def input = slurper.parseText(reader.readUntil('\0'))

				"${input.type}"(input)
			}
		}
		catch (Exception e) {
			return false
		}
		return true
	}

	private broadcastNewNode(String address, int port) {
		nodes.findAll { it.key != address }*.value.each {
			it.outputStream.send('connect', [remoteAddress: address, remotePort: port])
		}
	}

	public void openConnection(Socket sock) {

		println "Connection from (me) ${sock.localAddress.hostAddress}:${sock.localPort} to (remote) ${sock.inetAddress.hostAddress}:${sock.port}"
		def node
		if ((node = nodes[sock.inetAddress.hostAddress]) && node.thread) {
			println "Closing existing connection to ${sock.inetAddress.hostAddress}"
			node.thread.interrupt()
			node.socket.close()
		}
		sock.withStreams { i, o ->
			def connectionInfo = [
				ip          : sock.inetAddress.hostAddress,
				outputStream: new PrintStream(o),
				socket      : sock,
				thread      : Thread.currentThread()
			]
			nodes[connectionInfo.ip] = connectionInfo

			connectionInfo.outputStream.metaClass.send = { type, map ->
				map.ip = connectionInfo.ip
				map.type = type
				connectionInfo.outputStream.print(JsonOutput.toJson(map))
				connectionInfo.outputStream.print('\0')
			}

			connectionInfo.outputStream.send('serverInfo', [name: sock.localAddress.hostName, connectionPort: listenPort])

			while (running && !Thread.currentThread().interrupted) {
				println "Handling input ${Thread.currentThread().id}"
				if (!handleInput(i)) {
					println "Connection to ${connectionInfo.name} (${connectionInfo.ip}:${sock.port}) failed"
					nodes[connectionInfo.ip].socket.close()
					nodes[connectionInfo.ip].remove 'socket'
					nodes[connectionInfo.ip].outputStream.close()
					nodes[connectionInfo.ip].remove 'outputStream'
					nodes[connectionInfo.ip].thread.interrupt()
					nodes[connectionInfo.ip].remove 'thread'

					break
				}
			}
		}
	}

	public void setListening(boolean on) {
		on ? startListening() : stopListening()
	}

	private void startListening() {
		if (!running) {
			listenThread = Thread.start {
				while (!Thread.currentThread().interrupted && running) {
					def s = socket.accept true, {
						openConnection(it)
					}
				}
				stopListening()
			}
			Thread.startDaemon {
				while (running) {
					println nodes
					nodes*.value.findAll { it.outputStream && it.socket }.each {
						try {
							it.outputStream.send('heartbeat', [
								timeStamp: Instant.now(Clock.systemUTC())
							])
						}
						catch (Exception e) {
							println e.message
						}
					}

					sleep(10000)
				}
			}
		}
	}

	private void stopListening() {
		if (running) {
			listenThread.interrupt()
			listenThread = null
		}
	}

	private void heartbeat(Map opts) {
		def heartbeatTime = LocalDateTime.now(Clock.systemUTC())
		opts.timeStamp = LocalDateTime.ofInstant(Instant.ofEpochSecond(opts.timeStamp.epochSecond, opts.timeStamp.nano), ZoneOffset.UTC)
		nodes[opts.ip].lastHeartbeat = opts.timeStamp
		println "heartbeat from ${opts.ip}: ${heartbeatTime}"
		println "round trip time: ${Duration.between(heartbeatTime, opts.timeStamp).abs()}"
	}

	private void serverInfo(Map opts) {
		nodes[opts.ip].connectionPort = opts.connectionPort
		nodes[opts.ip].name = opts.name

		broadcastNewNode(opts.ip, opts.connectionPort)

		println "server info from ${opts.ip}: name is ${opts.name}"
	}

	private void connectToNode(Map opts) {

		Thread.start {
			try {
				openConnection(new Socket(opts.remoteIp, opts.remotePort))
			}
			catch(Exception e) {
				println "Couldn't connect to ${opts.remoteIp}:${opts.remotePort}"
			}
		}
	}

	public static void main(String[] args) {
		def config = new JsonSlurper().parse(new File(args[0]))

		def MeshNode mn = new MeshNode(config.port)
		mn.listening = true

		config.remotes.each {
			mn.connectToNode(it)
		}
	}
}
