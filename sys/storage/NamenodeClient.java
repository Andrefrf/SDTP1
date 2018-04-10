package sys.storage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.glassfish.jersey.client.ClientConfig;

import api.storage.Namenode;


public class NamenodeClient implements Namenode {
	protected static InetAddress serverAddress;

	Trie<String, List<String>> names = new PatriciaTrie<>();
	
	public static void main(String[] args) throws IOException {
		
		ClientConfig config = new ClientConfig();
		Client client = ClientBuilder.newClient(config);
		
		final int port = 9000 ;
		final InetAddress group = InetAddress.getByName( args[0] ) ;

		if( ! group.isMulticastAddress()) {
		    System.out.println( "Not a multicast address (use range : 224.0.0.0 -- 239.255.255.255)");
		}

		byte[] data = "Namenode".getBytes();
		try(MulticastSocket socket = new MulticastSocket()) {
		    DatagramPacket request = new DatagramPacket( data, data.length, group, port ) ;
		    socket.send( request ) ; 
		    byte[] buffer = new byte[65536];
		    request = new DatagramPacket(buffer,buffer.length);
		    socket.receive(request);
		    serverAddress = request.getAddress();
		}
		URI baseURI = UriBuilder.fromPath(serverAddress.getHostName()).build();
		WebTarget target = client.target(baseURI);
		
		
	}
	
	@Override
	public List<String> list(String prefix) {
		
		return new ArrayList<>(names.prefixMap( prefix ).keySet());
	}

	@Override
	public void create(String name,  List<String> blocks) {
		if( names.putIfAbsent(name, new ArrayList<>(blocks)) != null )
			throw new WebApplicationException(Status.CONFLICT);
	}

	@Override
	public void delete(String prefix) {
		List<String> keys = new ArrayList<>(names.prefixMap( prefix ).keySet());
		if( ! keys.isEmpty() )
			names.keySet().removeAll( keys );
	}

	@Override
	public void update(String name, List<String> blocks) {
		if( names.putIfAbsent( name, new ArrayList<>(blocks)) == null ) {
			throw new WebApplicationException(Status.NOT_FOUND);
		}
	}

	@Override
	public List<String> read(String name) {
		List<String> blocks = names.get( name );
		if( blocks == null )
			throw new WebApplicationException(Status.NOT_FOUND);
		return blocks;
	}
}
