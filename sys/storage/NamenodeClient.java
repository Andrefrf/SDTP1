package sys.storage;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import api.storage.Namenode;

public class NamenodeClient implements Namenode {
	protected static InetAddress serverAddress;

	Trie<String, List<String>> names = new PatriciaTrie<>();
	WebTarget target;

	public NamenodeClient() {
		target = null;
	}

	@Override
	public List<String> list(String prefix) {
		return new ArrayList<>(names.prefixMap( prefix ).keySet());
	}

	@Override
	public void create(String name, List<String> blocks) {
		Response response = target.path( Namenode.PATH + name)
				.request()
				.post( Entity.entity( blocks, MediaType.APPLICATION_OCTET_STREAM));
		
		if( response.hasEntity() ) {
			String id = response.readEntity(String.class);
			System.out.println( "data resource id: " + id );
		} else
			System.err.println( response.getStatus() );
	}

	@Override
	public void delete(String prefix) {
		Response response = target.path( Namenode.PATH + "/list/").queryParam("prefix", prefix)
				.request()
				.delete();
		
		if( response.hasEntity() ) {
			String id = response.readEntity(String.class);
			System.out.println( id );
		} else
			System.err.println( response.getStatus() );
	}

	@Override
	public void update(String name, List<String> blocks) {
		Response response = target.path( Namenode.PATH + name)
				.request()
				.put( Entity.entity( blocks, MediaType.APPLICATION_OCTET_STREAM));
		
		if( response.hasEntity() ) {
			String id = response.readEntity(String.class);
			System.out.println( id );
		} else
			System.err.println( response.getStatus() );
	}

	@Override
	public List<String> read(String name) {
		Response response = target.path( Namenode.PATH + name)
				.request()
				.get();
		
		if( response.hasEntity() ) {
			//ha uma melhor maneira de tratar da list , mudar assim que se souber uma melhor maneira
			List<String> b = response.readEntity(new GenericType<List<String>>(){});
			System.out.println( "list: " + b );
			return b;
		} else
			System.err.println( response.getStatus() );
		return null;
	}
}
