package bitcoin.structure;

import java.io.Serializable;
import java.util.List;

/**
 * 
 *
 */
public class BitcoinScriptWitnessItem implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8500521021303513414L;
	private byte[] stackItemCounter;
	private List<BitcoinScriptWitness> scriptWitnessList;
	
	public BitcoinScriptWitnessItem(byte[] stackItemCounter, List<BitcoinScriptWitness> scriptWitnessList) {
		this.stackItemCounter=stackItemCounter;
		this.scriptWitnessList=scriptWitnessList;
	}
	
	public byte[] getStackItemCounter() {
		return this.stackItemCounter;
	}
	
	public List<BitcoinScriptWitness> getScriptWitnessList() {
		return this.scriptWitnessList;
	}
}
