/** page hash join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class HashJoin extends Join {

    int batchsize; // Number of tuples per out batch
	int leftBatchSize;
	int rightBatchSize;

    /**
     * The following fields are useful during execution of the NestedJoin operation
     **/
    int leftindex; // Index of the join attribute in left table
    int rightindex; // Index of the join attribute in right table

    String rfname; // The file name where the right table is materialize

    int filenum; // To get unique filenum for this operation

    Batch[] outBatches; // Output buffer for partition
	Batch inBatch;  // Input buffer for partition and join
	Batch[] hashTable;
    Batch leftBatches; // Buffers for left input stream
    Batch rightbatch; // Buffer for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file
	ObjectOutputStream out;

    int lcurs; // Cursor for left side buffer
    int rcurs; // Cursor for right side buffer
	int currentFile; 
    boolean eosl; // Whether end of stream (left table) is reached
    boolean eosr; // End of stream (right table)

    public HashJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes Materializes the right
     * hand side into a file Opens the connections
     **/

    public boolean open() {

        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
		leftBatchSize = Batch.getPageSize() / left.getSchema().getTupleSize();
		rightBatchSize = Batch.getPageSize() / right.getSchema().getTupleSize();

        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
		currentFile = -1;
		
        eosl = true;
        eosr = true;

		if(!partition(left, "Left", leftindex)) {
			return false;
		}
		if(!partition(right, "Right", rightindex)) {
			return false;
		}
		batchsize = Batch.getPageSize() / tuplesize;
		rfname = "HashJoin" + "right" + String.valueOf(currentFile);
		return true;
	}

	/**
	 * Partition the table into output buffers
	 **/

	public boolean partition(Operator table, String name, int index) {
		if (name.equal("left")) {
			batchsize = leftBatchSize;
		} else {
			batchsize = rightBatchSize;
		}

		/** reset the output buffers for partition **/
		outBatches = new Batch[numBuff - 1];
        for (int i = 0; i < numBuff - 1; i++) {
            outBatches[i] = new Batch(batchsize);
        }

		if(!table.open()) {
			return false;
		} else {
			//when there is still pages to fetch in the table
			while((inBatch = (Batch) table.next()) != null) {
				for(i = 0; i < inBatch.size(); i++) {
					Tuple tuple = inBatch.elementAt(i);
					int hash = Integer.valueOf(String.valueOf(tuple.dataAt(index))) % (numBuff-1);
					outBatches[hash].add(tuple);
					if(outBatches[hash].isFull()) {
						if(!outputBuffers(hash, name)) {
							return false;
						}
					}
				}
			}
		}
		/** all tuples has been hashed **/
		for(int i = 0; i < numBuff - 1; i++) {
			if(!outputBuffers(i, name)) {
				return false;
			}
		}
		
		if(!table.close()) {
			return false;
		}
		return true;
	}

	/**
	 * write the files in output buffers into files
	 **/

	public boolean outputBuffers(int index, String name) {
		rfname = "HashJoin" + name + String.valueOf(index);
		File file = new File(rfname);
		try {
			if(file.isFile()) {
				out = new AppendingObjectOutputStream(new FileOutputStream(rfname));
			} else {
				out = new ObjectOutputStream(new FileOutputStream(rfname));
			}
			out.writeObject(outBatches[index]);
			out.close();
		} catch (IOException io) {
			out.close();
			System.out.println("HashJoin: Writing the temporary file error");
            return false;
		}
		outBatches[index] = new Batch(batchsize);
		return true;
	}

    /**
     * from input buffers selects the tuples satisfying join condition And returns a
     * page of output tuples
     **/

    public Batch next() {
        // System.out.print("HashJoin:--------------------------in next----------------");
        // Debug.PPrint(con);
        // System.out.println();
		
        int i, j, k;
        
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {
			if (eosl && currentFile == numBuff - 2) {	//End of last left file, finished all join
				return (outbatch.isEmpty()) ? null : outbatch;
			}

			if (eosr) {  //End of right file, fetch new left file for probing of next partition
				if(eosl) {
					currentFile++;
				}
				fetchNewLeftFile();
				rfname = "HashJoin" + "right" + String.valueOf(currentFile);
				rcurs = 0;		//restart right cursor 
				lcurs = 0;		//restart left cursor

				try {
					in = new ObjectInputStream(new FileInputStream(rfname));
					eosr = false;	//not end of right file
				} catch (IOException io) {
					System.err.println("HashJoin:error in reading the right file");
					System.exit(1);
				}
			}

            while (!eosr) {
				try {
					do {
						inBatch = (Batch) in.readObject();
					} while (inBatch == null);
					rcurs = 0;
				} catch(EOFException eof) {
					try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("HashJoin:Error in closing file");
                    }
                    eosr = true;
				} catch (IOException io) {
                    System.out.println("HashJoin:right file reading error");
                    System.exit(1);
                }

				for (; rcurs < inBatch.size(); rcurs++) {
					//hash tuple
					Tuple righttuple = inBatch.elementAt(rcurs);
					int hash = Integer.valueOf(String.valueOf(tuple.dataAt(rightindex))) % (numBuff - 2);
					for(lcurs = 0; lcurs < hashTable[hash].size(); lcurs++) {
						Tuple lefttuple = hashTable[hash].elementAt(lcurs);
						if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
							Tuple outtuple = lefttuple.joinWith(righttuple);
							outbatch.add(outtuple);
							if(outbatch.isFull()) {
								return outbatch;
							}
						}
					}
				}
			}
		}
        return outbatch;
    }

    /** Close the operator */
    public boolean close() {
        File f = new File(rfname);
        if(f.isFile()) {
			f.delete();
		}
        return true;
    }

    /**
     * Fetches new block of left pages Returns false if end of left stream reached
     **/
    public void fetchNewLeftFile() {
		hashTable = newBatch[numBuff - 2];
		for (int i = 0; i < numBuff - 1; i++) {
            hashTable[i] = new Batch(leftBatchSize);
        }

		esol = false;
		rfname = "HashJoin" + "left" + String.valueOf(currentFile);
		try {
			in = new ObjectInputStream(new FileInputStream(rfname));
		} catch(IOException io) {
			System.err.println("HashJoin:error in reading the left file");
			System.exit(1);
		}

		do {
			inBatch = (Batch) in.readObject();
		} while (inBatch == null);

		boolean done = false;

		while (!done) {
			// done with one inBatch, fetch the next
			if(lcurs >= inBatch.size()) {
				try {
					lcurs = 0;
					do {
						inBatch = (Batch) in.readObject();
					} while (inBatch == null);

				} catch(EOFException eof) {
					try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("HashJoin:Error in closing file");
                    }
					close();
                    eosl = true;
				} catch (IOException io) {
                    System.out.println("HashJoin:left file reading error");
                    System.exit(1);
                }
			}

			for (; lcurs < inBatch.size(); lcurs++) {
				//hash tuple
				Tuple tuple = inBatch.elementAt(lcurs);
				int hash = Integer.valueOf(String.valueOf(tuple.dataAt(index))) % (numBuff - 2);

				//if hashtable full, store the rest of the tuple back to original file
				if(hashTable[hash].isFull()) {
					eosl = false;
					ObjectOutputStream temp = new ObjectOutputStream(new FileOutputStream("tempFile"));
					while(lcurs < inBatch.size()) {
						temp.writeObject(inBatch.elementAt(lcurs));
						lcurs++;
					}
					try {
						while(true){
							temp.writeObject(in.readObject());
						}
					} catch (EOFException eof) {
						try {
							in.close();
						} catch (IOException io) {
							System.out.println("HashJoin:Error in closing file");
						}
						close();
						temp.close();
					}
					out = new ObjectOutputStream(new FileOutputStream(rfname));
					in = new ObjectInputStream(new FileInputStream("tempFile"));
					try{
						while(true) {
							out.writeObject(in.readObject());
						}
					} catch(EOFException eof) {
						try {
							in.close();
							File f = new File("tempFile");
							f.delete();
							out.close();
						} catch (IOException io) {
							System.out.println("HashJoin:Error in closing file");
						}
					}
					done = true;
				} else {
					hashTable[hash].add(tuple);
				}
			}
		}
    }

}

public class AppendingObjectOutputStream extends ObjectOutputStream {

  public AppendingObjectOutputStream(OutputStream out) throws IOException {
    super(out);
  }

  @Override
  protected void writeStreamHeader() throws IOException {
    reset();
  }

}