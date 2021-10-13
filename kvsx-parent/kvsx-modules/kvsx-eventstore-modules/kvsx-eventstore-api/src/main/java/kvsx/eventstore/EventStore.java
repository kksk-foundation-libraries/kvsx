package kvsx.eventstore;

import kvsx.bytearray.ByteArrayStore;
import lombok.Builder;

@Builder(builderClassName = "Builder")
public class EventStore {
	private final ByteArrayStore byteArrayStore;
}
