package sys.storage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import api.storage.Datanode;
import utils.Random;


public class DatanodeServer implements Datanode {
	
	private static final int INITIAL_SIZE = 32;
	private Map<String, byte[]> blocks = new HashMap<>(INITIAL_SIZE);

	public static void main(String[] args) throws IOException {

		ResourceConfig config = new ResourceConfig();
		config.register(new DatanodeServer());

		final int MAX_DATAGRAM_SIZE = 65536;
		final InetAddress group = InetAddress.getByName(args[0]);
		URI serverURI = UriBuilder.fromUri(group.getHostName()).build();
		JdkHttpServerFactory.createHttpServer(serverURI, config);
		String serverPath = serverURI.getPath();

		if (!group.isMulticastAddress()) {
			System.out.println("Not a multicast address (use range : 224.0.0.0 -- 239.255.255.255)");
			System.exit(1);
		}

		System.err.println("Server ready....");
		
		try (MulticastSocket socket = new MulticastSocket(9000)) {
			socket.joinGroup(group);
			while (true) {
				byte[] buffer = new byte[MAX_DATAGRAM_SIZE];
				DatagramPacket request = new DatagramPacket(buffer, buffer.length);
				socket.receive(request);
				InetAddress path = request.getAddress();
				int port = request.getPort();		
				if(!request.getData().equals("Datanode")){
					continue;
				}
				System.out.write(request.getData(), 0, request.getLength());
				request = new DatagramPacket(serverPath.getBytes(), serverPath.getBytes().length,
						path, port);
				socket.send(request);
			}
		}
	}

	@Override
	public String createBlock(byte[] data) {
		String id = Random.key64();
		blocks.put(id, data);
		return id;
	}

	@Override
	public void deleteBlock(String block) {
		blocks.remove(block);
	}

	@Override
	public byte[] readBlock(String block) {
		byte[] data = blocks.get(block);
		if (data != null)
			return data;
		else
			throw new WebApplicationException(Status.NOT_FOUND);
	}
}
