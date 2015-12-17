package eu.fbk.pikesir.dbpedia;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * User: aprosio
 * Date: 10/25/12
 * Time: 11:56 AM
 * To change this template use File | Settings | File Templates.
 */
public class DBpediaOntologyNode {
	public HashMap<String, String> labels = new HashMap<String, String>();
	public HashSet<String> equivalentClasses = new HashSet<String>();
	public String superClass = null;
	public String className = null;
	public Integer id = null;
	public HashSet<HashMap<String, String>> properties = new HashSet<HashMap<String, String>>();

	public HashSet<DBpediaOntologyNode> children = new HashSet<DBpediaOntologyNode>();
	public DBpediaOntologyNode parent = null;

	private int RECURSION_LIMIT = 10;

	public DBpediaOntologyNode(String className, String superClass) {
		this.className = className;
		this.labels = new HashMap<>();
		this.equivalentClasses = new HashSet<>();
		this.superClass = superClass;
	}

	public DBpediaOntologyNode(String className, HashMap<String, String> labels, HashSet<String> equivalentClasses, String superClass) {
		this.className = className;
		this.labels = labels;
		this.equivalentClasses = equivalentClasses;
		this.superClass = superClass;
	}

	public HashSet<DBpediaOntologyNode> getChildrenRecursively() {
		HashSet<DBpediaOntologyNode> ret = new HashSet<DBpediaOntologyNode>();
		for (DBpediaOntologyNode n : children) {
			ret.add(n);
			ret.addAll(n.getChildrenRecursively());
		}
		return ret;
	}

	public String toStringRecursive() {
		return toStringRecursive(0);
	}

	public String toStringRecursive(int tabs) {
		StringBuffer ret = new StringBuffer();
		if (tabs > RECURSION_LIMIT) {
			return ret.toString();
		}
		for (int i = 0; i < tabs; i++) {
			ret.append("\t");
		}
		ret.append(className);
		ret.append("\n");
		for (DBpediaOntologyNode n : children) {
			ret.append(n.toStringRecursive(tabs + 1));
		}

		return ret.toString();
	}

	public String toString() {
		String ret = className;
/*
		String ret = "";
        ret += "Name: " + className;
        ret += " - Super: " + superClass;
        ret += " - Labels: " + labels;
        ret += " - Equivalent classes: " + equivalentClasses;
*/

		return ret;
	}
}

