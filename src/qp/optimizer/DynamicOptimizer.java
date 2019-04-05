/** performs dynamic optimization, iterative improvement algorithm **/

package qp.optimizer;

import qp.utils.*;
import qp.operators.*;
import java.lang.Math;
import java.util.*;
import java.io.*;

public class DynamicOptimizer {

	SQLQuery sqlquery;
	int numTable;
	ArrayList<HashMap<ArrayList<Integer>, Operator>> optPlan;
	Vector fromList;
	int cost;
	Operator root;
	int currentCost, joinType;

	public DynamicOptimizer(SQLQuery sqlquery) {
		this.sqlquery = sqlquery;
		fromList = (Vector) sqlquery.getFromList();
		numTable = fromList.size();
		optPlan = new ArrayList<HashMap<ArrayList<Integer>, Operator>>();
		for (int i = 0; i < numTable; i++) {
			optPlan.add(new HashMap<ArrayList<Integer>, Operator>());
		}
	}

	public Operator getOptimizedPlan() {
		for(int i = 1; i <= numTable; i++) {
		    ArrayList<Integer> r = new ArrayList<Integer>();
		    r.add(i-1);
			optPlan.get(0).put(r, accessPlan(i-1));

			if(optPlan.get(0).get(r) == null) { System.out.println("numaahahha"); }
		}
		System.out.println("size = " + optPlan.get(0).size());
		for(int i = 2; i <= numTable; i++) {
			//get all subset of size i
			ArrayList<ArrayList<Integer>> subset = getSubset(i);
            for(int p = 0; p <subset.size(); p++) {
                for (int j = 0; j <subset.get(p).size(); j++){
                    System.out.print(subset.get(p).get(j));
                }
                System.out.println();
            }

			for(int j = 0; j < subset.size(); j++) {
				Operator bestPlan = null;
				int bestCost = Integer.MAX_VALUE;
				ArrayList<Integer> sub = subset.get(j);

				//for each subset of size i, get different pairs of s and r
				for(int k = 0; k < sub.size(); k++) {
                    ArrayList<Integer> r = new ArrayList<Integer>();
                    ArrayList<Integer> s = new ArrayList<Integer>();
					r.add(sub.get(k));
					for(int g = 0; g < sub.size(); g++) {
						if(g != k) {
							s.add(sub.get(g));
						}
					}
					System.out.println("s = " + s);
					System.out.println("r = " + r);
					if(bestPlan != null) {
                        System.out.print("best plan4 = ");
                        Debug.PPrint(bestPlan);
                        System.out.println();
                    }
					Operator p = joinPlan(optPlan.get(i-2).get(s), optPlan.get(0).get(r));
					System.out.println("cost = " + cost);


					if (p != null && bestCost > cost) {
                        System.out.println("r = " + r);
						bestPlan = p;
						bestCost = cost;
                        System.out.print("best plan1 = ");
                        Debug.PPrint(bestPlan);
                        System.out.println();
					}
				}
                if(bestPlan != null) {
                    System.out.print("best plan before storing = ");
                    Debug.PPrint(bestPlan);
                    System.out.println();
                }
				optPlan.get(i-1).put(sub, bestPlan);
			}
		}
		createProjectOp(optPlan.get(numTable - 1).get(getSubset(numTable).get(0)));
		System.out.print("final optimised plan = ");
        Debug.PPrint(root);
        System.out.println();
		return root;
	}

	/** get best plan for one relation **/
	private Operator accessPlan(int i) {
		String tableName = (String) fromList.elementAt(i);
		System.out.println("tableName = " + tableName);
		root = null;
		createScanOp(tableName);
		createSelectOp(tableName);
		if(root == null) { System.out.println("i = " + i); }
		return root;
	}

	/**
	 * Create Scan Operator for table
	 **/

	private void createScanOp(String tableName) {
		Scan op1 = new Scan(tableName, OpType.SCAN);

		/**
		 * Read the schema of the table from tablename.md file md stands for metadata
		 **/

		String filename = tableName + ".md";
		try {
			ObjectInputStream _if = new ObjectInputStream(new FileInputStream(filename));
			Schema schm = (Schema) _if.readObject();
			op1.setSchema(schm);
			_if.close();
		} catch (Exception e) {
			System.err.println("RandomInitialPlan:Error reading Schema of the table" + filename);
			System.exit(1);
		}


		root = op1;
        System.out.print("scan plan = ");
        Debug.PPrint(root);
        System.out.println();
        if(root == null){System.out.println("nullScan");}
	}

	/**
	 * Create Selection Operators for each of the selection condition mentioned in
	 * Condition list
	 **/

	private void createSelectOp(String tableName) {
		Select op1 = null;
		Vector selectionlist = sqlquery.getSelectionList();
		System.out.println("size1 = " + selectionlist.size());
        System.out.println("size1 = " + sqlquery.getConditionList().size());
        System.out.println("size1 = " + sqlquery.getJoinList().size());

		for (int j = 0; j < selectionlist.size(); j++) {
			Condition cn = (Condition) selectionlist.elementAt(j);
			System.out.println("OpType = " + cn.getOpType());
			if (cn.getOpType() == Condition.SELECT) {
				String tabname = cn.getLhs().getTabName();
				// System.out.println("RandomInitial:-------------Select-------:"+tabname);
				if(tabname.equals(tableName)){
					op1 = new Select(root, cn, OpType.SELECT);
					/** set the schema same as base relation **/
					op1.setSchema(root.getSchema());
				}
			}
		}
        if (selectionlist.size() != 0)
            root = op1;
        System.out.print("select plan = ");
        Debug.PPrint(root);
        System.out.println();
        if(root == null){System.out.println("nullSelect");}
	}

	private void createProjectOp(Operator plan) {
        System.out.print("best project plan = ");
        Debug.PPrint(plan);
        System.out.println();

		Vector projectlist = (Vector) sqlquery.getProjectList();

		if (projectlist == null){
		    System.out.println("projecList is null");
            projectlist = new Vector();
        }


		if (!projectlist.isEmpty()) {
		    System.out.println("project is not empty");
			root = new Project(plan, projectlist, OpType.PROJECT);
			Schema newSchema = plan.getSchema().subSchema(projectlist);
			root.setSchema(newSchema);
		} else {
            root = plan;
        }
	}

	/** join S with R to get new plan in the best possible way **/
	private Operator joinPlan(Operator s, Operator r) {
	    if(s == null || r == null) {
	        return null;
        }
		Vector joinlist = sqlquery.getJoinList();
		Condition condition = null;
		cost = Integer.MAX_VALUE;
		PlanCost plancost = new PlanCost();
		Join jn = null;

		for(int j = 0; j < joinlist.size(); j++) {
			Condition cn = (Condition) joinlist.elementAt(j);
			Attribute leftAttr = cn.getLhs();
			Attribute rightAttr = (Attribute) cn.getRhs();

			if(s.getSchema().contains(leftAttr) && r.getSchema().contains(rightAttr)){
				condition = (Condition)cn.clone();
				break;
			} else if(s.getSchema().contains(rightAttr) && r.getSchema().contains(leftAttr)) {
				condition = (Condition)cn.clone();
				condition.setLhs(rightAttr);
				condition.setRhs(leftAttr);
				break;
			}
		}

		if(condition != null) {
			jn = new Join(s, r, condition, OpType.JOIN);
			Schema newsche = s.getSchema().joinWith(r.getSchema());
			jn.setSchema(newsche);

			jn.setJoinType(JoinType.NESTEDJOIN);
			currentCost = plancost.getCost(jn);
			System.out.println("currCost for nested join = " + currentCost);
			updateJoin(JoinType.NESTEDJOIN);

            jn.setJoinType(JoinType.HASHJOIN);
            plancost = new PlanCost();
            currentCost = plancost.getCost(jn);
            System.out.println("currCost for hash join = " + currentCost);
            updateJoin(JoinType.HASHJOIN);

			jn.setJoinType(JoinType.BLOCKNESTED);
            plancost = new PlanCost();
			currentCost = plancost.getCost(jn);
            System.out.println("currCost for block join = " + currentCost);
			updateJoin(JoinType.BLOCKNESTED);



			jn.setJoinType(joinType);
		}
		return jn;
	}

	private void updateJoin(int i){
		if(currentCost < cost) {
			cost = currentCost;
			joinType = i;
		}
	}

	/** get all subset permutation of size i, subset are sorted**/
	private ArrayList<ArrayList<Integer>> getSubset(int i) {
        ArrayList<Integer> sub;
        ArrayList<ArrayList<Integer>> finalSub = new ArrayList<ArrayList<Integer>>();
	    int[] combi = new int[i];
	    for (int j = 0; j < i; j++) {
	        combi[j] = j;
        }
        int j = i - 1;
	    while (combi[0] < numTable - i + 1) {
	        while(j > 0 && combi[j] == numTable - i + j) {
	            j--;
            }

            sub = new ArrayList<Integer>();
            for(int k = 0; k < i; k++) {
	            sub.add(combi[k]);
            }
            finalSub.add(sub);

            combi[j]++;
            while(j < i - 1) {
                combi[j+1] = combi[j] + 1;
                j++;
            }
        }
		/*
		for (int k = 0; k <= numTable - i; k++) {
		    for(int p = 0; p < i && k + 1 + p < numTable; p++) {
                sub = new ArrayList<Integer>();
                sub.add(k);
                for (int j = k + 1 + p; j < k + i + p; j++) {
                    sub.add(j);
                }
                finalSub.add(sub);
            }
		}*/
		return finalSub;
	}
}