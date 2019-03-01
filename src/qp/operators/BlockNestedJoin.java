/** page nested join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class BlockNestedJoin extends Join {

    int batchsize; // Number of tuples per out batch

    /**
     * The following fields are useful during execution of the NestedJoin operation
     **/
    int leftindex; // Index of the join attribute in left table
    int rightindex; // Index of the join attribute in right table

    String rfname; // The file name where the right table is materialize

    static int filenum = 0; // To get unique filenum for this operation

    Batch outbatch; // Output buffer
    Batch[] leftBatches; // Buffers for left input stream
    Batch rightbatch; // Buffer for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lnum; // Block number for left side buffers
    int lcurs; // Cursor for left side buffer
    int rcurs; // Cursor for right side buffer
    boolean eosl; // Whether end of stream (left table) is reached
    boolean eosr; // End of stream (right table)

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }
    
    /** 
     * Number of buffers available to this join operator 
     * Overrides method in Join 
     **/
    
    public void setNumBuff(int num){
        this.numBuff = num;
        leftBatches = new Batch[num - 2];
        for (int i = 0; i < num - 2; i++) {
            leftBatches[i] = new Batch(batchsize);
        }
    }

    /**
     * During open finds the index of the join attributes Materializes the right
     * hand side into a file Opens the connections
     **/

    public boolean open() {

        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
        lnum = 0;
        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /**
         * because right stream is to be repetitively scanned if it reached end, we have
         * to start new scan
         **/
        eosr = true;

        /**
         * Right hand side table is to be materialized for the Nested join to perform
         **/

        if (!right.open()) {
            return false;
        } else {
            /**
             * If the right operator is not a base table then Materialize the intermediate
             * result from right into a file
             **/

            // if(right.getOpType() != OpType.SCAN){
            filenum++;
            rfname = "NJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin:Writing the temporary file error");
                return false;
            }
            // }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition And returns a
     * page of output tuples
     **/

    public Batch next() {
        // System.out.print("BlockNestedJoin:--------------------------in next----------------");
        // Debug.PPrint(con);
        // System.out.println();
        int i, j, k;
        if (eosl) {
            close();
            return null;
        }
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {

            if (lcurs == 0 && lnum == 0 && eosr == true) { // End of last left page and end of right file
                if (!fetchNewLeftBlock()) {
                    eosl = true;
                    return outbatch;
                }
            }

            while (eosr == false) {

                try {
                    /* When all tuples in last left page and right page checked, fetch next right page */
                    if (rcurs == 0 && lnum == 0 && lcurs == 0) { 
                        rightbatch = (Batch) in.readObject();
                    }

                    for (i = lnum; i < leftBatches.length; i++) {

                        /* If left buffer is empty */
                        if (leftBatches[i] == null) {
                            break;
                        }

                        for (j = lcurs; j < leftBatches[i].size(); j++) {
                            for (k = rcurs; k < rightbatch.size(); k++) {
                                Tuple lefttuple = leftBatches[i].elementAt(j);
                                Tuple righttuple = rightbatch.elementAt(k);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    // Debug.PPrint(lefttuple);
                                    // Debug.PPrint(righttuple);
                                    Tuple outtuple = lefttuple.joinWith(righttuple);

                                    // Debug.PPrint(outtuple);
                                    // System.out.println();
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        if (j == leftBatches[i].size() - 1 && k == rightbatch.size() - 1) { // case 1
                                            lcurs = 0;
                                            rcurs = 0;
                                            if (i != numBuff-3) { // last left buffer
                                                lnum = i + 1;
                                            } else {
                                                lnum = 0;
                                            }
                                        } else if (j != leftBatches[i].size() - 1 && k == rightbatch.size() - 1) { // case 2
                                            lcurs = j + 1;
                                            rcurs = 0;
                                            lnum = i;
                                        } else if (j == leftBatches[i].size() - 1 && k != rightbatch.size() - 1) { // case 3
                                            lcurs = j;
                                            rcurs = k + 1;
                                            lnum = i;
                                        } else {
                                            lcurs = j;
                                            rcurs = k + 1;
                                            lnum = i;
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs = 0;
                        }
                        lcurs = 0;
                    }
                    lnum = 0;

                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin:Error in temporary file reading");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin:Some error in deserialization ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin:Temporary file reading error");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    /** Close the operator */
    public boolean close() {

        File f = new File(rfname);
        f.delete();
        return true;

    }

    /**
     * Fetches new block of left pages Returns false if end of left stream reached
     **/
    public boolean fetchNewLeftBlock() {
        for (int toFetch = 0; toFetch < numBuff - 2; toFetch++) {
            leftBatches[toFetch] = (Batch) left.next();

            if (toFetch == 0 && leftBatches[toFetch] == null) { // no left pages
                return false;
            }
            /**
             * Whenever a block of new left pages is loaded, we have to start the scanning
             * of right table
             **/
            try {

                in = new ObjectInputStream(new FileInputStream(rfname));
                eosr = false;
            } catch (IOException io) {
                System.err.println("BlockNestedJoin:error in reading the file");
                System.exit(1);
            }
        }
        return true;
    }

}
