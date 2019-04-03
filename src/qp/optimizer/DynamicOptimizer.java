/** performs dynamic optimization, iterative improvement algorithm **/

package qp.optimizer;

import qp.utils.*;
import qp.operators.*;
import java.lang.Math;
import java.util.*;

public class DynamicOptimizer {

	SQLQuery sqlquery;
	int numTable;
	Map<ArrayList<Integer>, Operator>[] optPlan;
	Vector fromList;
	int cost;
	Operator root;
	int currentCost, joinType;

	public DynamicOptimizer(SQLQuery sqlquery) {
		this.sqlquery = sqlquery;
		fromList = (Vector) sqlquery.getFromList();
		numTable = fromList.size();
		optPlan = new Map<ArrayList<Integer>, Operator>[numTable];
		for (int i = 0; i < numTable; i++) {
			optPlan[i] = new Map<ArrayList<Integer>, Operator>();
		}
	}

	public Operator getOptimizedPlan() {
		for(int i = 1; i <= numTable; i++) {
			optPlan[0].put(new ArrayList<Integer>(i-1), accessPlan(i-1));
		}
		for(int i = 2; i <= numTable; i++) {
			//get all subset of size i
			ArrayList<ArrayList<Integer>> subset = getSubset(i);

			for(int j = 0; j < subset.size(); j++) {
				Operator bestPlan = new Operator();
				int bestCost = Integer.MAX_VALUE;
				ArrayList<Integer> sub = subset.get(j);
				int r;
				ArrayList<Integer> s;
				//for each subset of size i, get different pairs of s and r
				for(int k = 0; k < sub.size(); k++) {
					r = sub.get(k);
					for(int g = 0; g < sub.size(); g++) {
						if(g != k) {
							s.add(sub.get(g));
						}
					}
					Operator p = joinPlan(optPlan[i-2].get(s), optPlan[0].get(r));
					if (p != null && bestCost > cost) {
						bestPlan = p;
						bestCost = cost;
					}
				}
				optPlan[i-1].put(sub, bestPlan);
			}
		}
		createProjectOp(optPlan[numTable - 1].get(getSubset(numTable).get(0)));
		return root;
	}

	/** get best plan for one relation **/
	private Operator accessPlan(int i) {
		String tableName = (String) fromlist.elementAt(i);
		root = new Operator();
		createScanOp(tableName);
		createSelectOp(tableName);
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

		String filename = tabname + ".md";
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
	}

	/**
	 * Create Selection Operators for each of the selection condition mentioned in
	 * Condition list
	 **/

	private void createSelectOp(String tableName) {
		Select op1 = null;
		Vector selectionlist = sqlquery.getSelectionList();

		for (int j = 0; j < selectionlist.size(); j++) {
			Condition cn = (Condition) selectionlist.elementAt(j);
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
		root = op1;
	}

	private void createProjectOp(Operator root) {
		Operator base = root;
		Vector projectlist = (Vector) sqlquery.getProjectList();

		if (projectlist == null)
			projectlist = new Vector();

		if (!projectlist.isEmpty()) {
			root = new Project(base, projectlist, OpType.PROJECT);
			Schema newSchema = base.getSchema().subSchema(projectlist);
			root.setSchema(newSchema);
		}
	}

	/** join S with R to get new plan in the best possible way **/
	private Operator joinPlan(Operator s, Operator r) {
		Vector joinlist = sqlquery.getJoinList();
		Condition condition = null;
		cost = Integer.MAX_VALUE;
		PlanCost plancost = new PlanCost();
		Join jn = null;

		for(int j = 0; j < joinlist.size(); i++) {
			Condition cn = (Condition) joinlist.elementAt(j);
			Attribute leftAttr = cn.getLhs();
			Attribute rightAttr = (Attribute) cn.getRhs();
			if(s.getSchema().contains(leftAttr) && r.getSchema().contains(rightAttr)){
				condition = cn;
				break;
			} else if(s.getSchema().contains(rightAttr) && r.getSchema().contains(leftAttr)) {
				condition = cn;
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
			updateJoin(JoinType.NESTEDJOIN);

			jn.setJoinType(JoinType.BLOCKNESTED);
			currentCost = plancost.getCost(jn);
			updateJoin(JoinType.BLOCKNESTED);

			jn.setJoinType(JoinType.HASHJOIN);
			currentCost = plancost.getCost(jn);
			updateJoin(JoinType.HASHJOIN);

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
		ArrayList<ArrayList<Integer>> finalSub = new ArrayList<Integer>();
		for (int k = 0; k <= numTable - i; k++) {
			sub = new ArrayList<Integer>();
			for(int j = 0; j < i; j++) {
				sub.add(k + j);
			}
			finalSub.add(sub);
		}
		return finalSub;
	}
}