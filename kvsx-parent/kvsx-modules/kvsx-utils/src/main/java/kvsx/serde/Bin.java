package kvsx.serde;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class Bin {
	private byte[] data;
}
