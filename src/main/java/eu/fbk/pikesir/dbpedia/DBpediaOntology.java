package eu.fbk.pikesir.dbpedia;

import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBpediaOntology {

	HashMap<String, String> singleDomainOntology = new HashMap<String, String>();
	HashMap<String, String> singleRangeOntology = new HashMap<String, String>();
	HashMap<String, Set<String>> completeOntology = new HashMap<String, Set<String>>();
	HashSet<DBpediaOntologyNode> nodes = new HashSet<DBpediaOntologyNode>();
	HashMap<String, HashMap<String, String>> properties = new HashMap<String, HashMap<String, String>>();
	HashMap<String, DBpediaOntologyNode> indexedNodes = new HashMap<String, DBpediaOntologyNode>();

	private boolean lowerProp = false;

	public boolean isLowerProp() {
		return lowerProp;
	}

	public void setLowerProp(boolean lowerProp) {
		this.lowerProp = lowerProp;
	}

	HashMap<String, Integer> depths = new HashMap<>();
	boolean loadedDephts = false;

	public static Pattern genericDBpediaPattern = Pattern.compile("^http....?.?.?dbpedia.org/[a-z0-9_-]+/(.*)$");
	public static Pattern foafPattern = Pattern.compile("^http....?.?.?xmlns.com/foaf/[0-9\\.]+/(.*)$");

	String ontologyFile;

	public HashMap<String, String> getProperty(String propName) {
		return properties.get(propName);
	}

	public HashMap<String, HashMap<String, String>> getProperties() {
		return properties;
	}

	public HashSet<DBpediaOntologyNode> getNodes() {
		return nodes;
	}

	public boolean isLoadedDephts() {
		return loadedDephts;
	}

	public int getDepth(String c) {
		if (!loadedDephts) {
			loadDepths();
		}
		return depths.get(c);
	}

	public void loadDepths() {
		for (DBpediaOntologyNode n : nodes) {
			if (n.className == null) {
				continue;
			}

			depths.put(n.className, getHistoryFromName(n.className).size());
		}

		loadedDephts = true;
	}

//	public int compareDephts(String c1, String c2) {
//		if (!loadedDephts) {
//			loadDepths();
//		}
//		int dim1 = depths.get(c1);
//	}

	public HashSet<String> completeClasses(HashSet<String> tmpClasses) {
		HashSet<String> classes = new HashSet<>();
		if (tmpClasses != null) {
			for (String c : tmpClasses) {
				String[] parts = c.split("/");
				for (String s : parts) {
					List<DBpediaOntologyNode> nodes = getHistoryFromName(s);
					if (nodes == null) {
//						logger.trace(String.format("Error in class %s", s));
						continue;
					}
					for (DBpediaOntologyNode n : nodes) {
						classes.add(n.className);
					}
				}
			}
		}

		return classes;
	}

	protected void removeNode(DBpediaOntologyNode node) {
		for (DBpediaOntologyNode child : node.children) {
			removeNode(child);
		}
		nodes.remove(node);
	}

	private ArrayList<DBpediaOntologyNode> getHistoryFromNode(DBpediaOntologyNode node, String stopClass, int limit) {
		ArrayList<DBpediaOntologyNode> ret = new ArrayList<DBpediaOntologyNode>();
		if (limit > 10) {
			return null;
		}
		if (node != null && (stopClass == null || (stopClass != null && !node.className.equals(stopClass)))) {
			ret.add(node);
			ret.addAll(getHistoryFromNode(node.parent, stopClass, limit - 1));
		}
		return ret;
	}

	public static Integer isInside(ArrayList<DBpediaOntologyNode> first, ArrayList<DBpediaOntologyNode> second) {
		// Lists must be reversed!
		int min = Math.min(first.size(), second.size());
		if (first.get(min - 1).equals(second.get(min - 1))) {
			if (second.size() > first.size()) {
				return 1;
			}
			else {
				return -1;
			}
		}
		return 0;
	}

	public ArrayList<DBpediaOntologyNode> getHistoryFromName(String name) {
		return getHistoryFromName(name, null, 0);
	}

	public ArrayList<DBpediaOntologyNode> getHistoryFromName(String name, String stopClass) {
		return getHistoryFromName(name, stopClass, 0);
	}

	private ArrayList<DBpediaOntologyNode> getHistoryFromName(String name, String stopClass, int limit) {
		DBpediaOntologyNode thisNode = getNodeByName(name);
		if (thisNode == null) {
			return null;
		}
		return getHistoryFromNode(thisNode, stopClass, limit);
	}

	public DBpediaOntologyNode getNodeByName(String name) {
		for (DBpediaOntologyNode node : nodes) {
			if (node.className.equals(name)) {
				return node;
			}
			for (String altName : node.equivalentClasses) {
				if (node.className.equals(altName)) {
					return node;
				}
			}
		}
		return null;
	}

	public HashSet<DBpediaOntologyNode> getRootNodes() {
		HashSet<DBpediaOntologyNode> ret = new HashSet<DBpediaOntologyNode>();
		for (DBpediaOntologyNode node : nodes) {
			if (node.superClass == null) {
				ret.add(node);
			}
		}
		return ret;
	}

	public HashSet<DBpediaOntologyNode> getLeafNodes() {
		HashSet<DBpediaOntologyNode> ret = new HashSet<DBpediaOntologyNode>();
		for (DBpediaOntologyNode node : nodes) {
			if (node.children.size() == 0) {
				ret.add(node);
			}
		}
		return ret;
	}

	public static String cleanName(String s, String toBeFound) {
		if (s == null) {
			return null;
		}
		int j = s.lastIndexOf(toBeFound);
		if (j != -1) {
			s = s.substring(j + toBeFound.length());
		}
		else {
			s = null;
		}
		return s;
	}

	public static String cleanName(String s) {
		return cleanName(s, "http://dbpedia.org/ontology/");
	}

	public static String cleanGenericName(String s) {
		Matcher m;

		m = genericDBpediaPattern.matcher(s);
		if (m.find()) {
			return m.group(1);
		}

		m = foafPattern.matcher(s);
		if (m.find()) {
			return "foaf:" + m.group(1);
		}
		return s;
	}

	public DBpediaOntology(String ontologyFile) {
		this(ontologyFile, false);
	}

	public DBpediaOntology(String ontologyFile, boolean lower) {
		this.ontologyFile = ontologyFile;
		this.lowerProp = lower;
		File d2 = new File(ontologyFile);
		if (!d2.exists()) {
			System.err.println("Ontology file does not exist");
			System.exit(1);
		}
		readOntologyType();
	}

	public void readOntologyType() {
		try {

			File fXmlFile = new File(ontologyFile);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
			doc.getDocumentElement().normalize();

			// Properties

			NodeList ndProperties = doc.getElementsByTagName("owl:DatatypeProperty");
			for (int temp = 0; temp < ndProperties.getLength(); temp++) {
				Node nNode = ndProperties.item(temp);

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					String propertyName = eElement.getAttribute("rdf:about");
					propertyName = cleanName(propertyName);

					if (propertyName == null) {
						continue;
					}
					if (propertyName.contains("/")) {
						continue;
					}

					// propertyName = FuzzyTokenizer.tokenizeToString(propertyName);

					String domain = null, range = null;

					NodeList list = nNode.getChildNodes();
					if (list.getLength() > 0) {
						for (int i = 0; i < list.getLength(); i++) {
							Node subNode = list.item(i);
							String nodeName = subNode.getNodeName();
							if (nodeName.equals("rdfs:domain")) {
								domain = ((Attr) subNode.getAttributes().getNamedItem("rdf:resource")).getValue();
							}
							if (nodeName.equals("rdfs:range")) {
								range = ((Attr) subNode.getAttributes().getNamedItem("rdf:resource")).getValue();
							}
						}
					}

					// Domain must be in dbpedia
					String simpleDomain = cleanName(domain);
					String simpleRange = cleanName(range, "http://www.w3.org/2001/XMLSchema#");

//                    if (simpleRange == null || simpleDomain == null) {
//                        continue;
//                    }

					if (lowerProp) {
						propertyName = propertyName.toLowerCase();
					}
					properties.put(propertyName, new HashMap<String, String>());
					properties.get(propertyName).put("name", propertyName);
					properties.get(propertyName).put("domain", simpleDomain);
					properties.get(propertyName).put("range", simpleRange);
					properties.get(propertyName).put("type", "data");
				}
			}

			NodeList noProperties = doc.getElementsByTagName("owl:ObjectProperty");
			for (int temp = 0; temp < noProperties.getLength(); temp++) {
				Node nNode = noProperties.item(temp);

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					String propertyName = eElement.getAttribute("rdf:about");
					propertyName = cleanName(propertyName);

					if (propertyName == null) {
						continue;
					}
					if (propertyName.contains("/")) {
						continue;
					}

					// propertyName = FuzzyTokenizer.tokenizeToString(propertyName).toLowerCase();

					String domain = null, range = null;

					NodeList list = nNode.getChildNodes();
					if (list.getLength() > 0) {
						for (int i = 0; i < list.getLength(); i++) {
							Node subNode = list.item(i);
							String nodeName = subNode.getNodeName();
							if (nodeName.equals("rdfs:domain")) {
								domain = ((Attr) subNode.getAttributes().getNamedItem("rdf:resource")).getValue();
							}
							if (nodeName.equals("rdfs:range")) {
								range = ((Attr) subNode.getAttributes().getNamedItem("rdf:resource")).getValue();
							}
						}
					}

					// Domain must be in dbpedia
					String simpleDomain = cleanName(domain);
					String simpleRange = cleanName(range);

//                    if (simpleRange == null || simpleDomain == null) {
//                        continue;
//                    }

					if (lowerProp) {
						propertyName = propertyName.toLowerCase();
					}
					properties.put(propertyName, new HashMap<String, String>());
					properties.get(propertyName).put("name", propertyName);
					properties.get(propertyName).put("domain", simpleDomain);
					properties.get(propertyName).put("range", simpleRange);
					properties.get(propertyName).put("type", "object");
				}
			}

			// Classes

			NodeList nListClass = doc.getElementsByTagName("owl:Class");

			for (int temp = 0; temp < nListClass.getLength(); temp++) {
				Node nNode = nListClass.item(temp);

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;

					String className = eElement.getAttribute("rdf:about");
					className = cleanName(className);

					HashMap<String, String> labels = new HashMap<String, String>();
					HashSet<String> equivalentClasses = new HashSet<String>();
					String superClass = null;

					NodeList list = nNode.getChildNodes();
					if (list.getLength() > 0) {
						for (int i = 0; i < list.getLength(); i++) {
							Node subNode = list.item(i);
							String nodeName = subNode.getNodeName();
							if (nodeName == "rdfs:subClassOf") {
								String superClassTmp = ((Attr) subNode.getAttributes().getNamedItem("rdf:resource")).getValue();
								superClassTmp = cleanName(superClassTmp);
								if (superClassTmp != null) {
									superClass = superClassTmp;
								}
							}
							if (nodeName == "owl:equivalentClass") {
								String s = ((Attr) subNode.getAttributes().getNamedItem("rdf:resource")).getValue();
								s = cleanName(s);
								if (s != null) {
									equivalentClasses.add(s);
								}
							}
							if (nodeName == "rdfs:label") {
								String lang = ((Attr) subNode.getAttributes().getNamedItem("xml:lang")).getValue();
								String text = subNode.getTextContent();
								labels.put(lang, text);
							}
						}
					}

					DBpediaOntologyNode n = new DBpediaOntologyNode(className, labels, equivalentClasses, superClass);
					nodes.add(n);
					indexedNodes.put(className, n);
				}
			}

			for (String p : properties.keySet()) {
				if (properties.get(p).get("domain") != null) {
					try {
						indexedNodes.get(properties.get(p).get("domain")).properties.add(properties.get(p));
					} catch (Exception ignored) {
//						System.out.println("ERROR");
//						System.out.println(properties.get(p).get("domain"));
//						System.out.println(indexedNodes.get(properties.get(p).get("domain")));
					}
				}
			}

			updateTree();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void updateTree() {
		for (DBpediaOntologyNode node : nodes) {
			node.parent = null;
			node.children = new HashSet<>();
		}

		for (DBpediaOntologyNode node : nodes) {
			if (node.superClass != null) {
				try {
					node.parent = this.getNodeByName(node.superClass);
					this.getNodeByName(node.superClass).children.add(node);
				} catch (Exception ignored) {
				}
			}
		}

		for (DBpediaOntologyNode node : nodes) {
			ArrayList<DBpediaOntologyNode> parents = getHistoryFromName(node.className);
			for (DBpediaOntologyNode n : parents) {
				node.properties.addAll(n.properties);
			}
		}
	}

	public void setDomainType(String relationName, String domainType) {
		singleDomainOntology.remove(relationName);
		singleDomainOntology.put(relationName, domainType);
	}

	public String getDomainType(String relationName) {
		return singleDomainOntology.get(relationName);
	}

	public HashMap<String, Set<String>> getCompleteOntology() {
		return completeOntology;
	}

	public void setRangeType(String relationName, String rangeType) {
		singleRangeOntology.remove(relationName);
		singleRangeOntology.put(relationName, rangeType);
	}

	public String getRangeType(String relationName) {
		return singleRangeOntology.get(relationName);
	}

	@Override
	public String toString() {
		StringBuffer ret = new StringBuffer();
		for (DBpediaOntologyNode n : getRootNodes()) {
			ret.append(n.toStringRecursive());
		}

		return ret.toString();
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("");
			System.out.println("USAGE:");
			System.out.println("");
			System.out.println("java -mx6G main.java.org.fbk.cit.hlt.moschitti.utils.DBpediaOntology\n" +
					" ontology-file\n" +
					"");
			System.out.println("");
			System.exit(1);
		}

		String ontology = args[0];
		DBpediaOntology o = new DBpediaOntology(ontology);

		HashSet<String> props = new HashSet<>();

		HashSet<String> types = new HashSet<>();
		types.add("date");
		types.add("dateTime");
		types.add("gYear");
		types.add("gYearMonth");

		for (String s : o.getProperties().keySet()) {
			if (o.getProperties().get(s).get("type").equals("object")) {
				continue;
			}
			if (types.contains(o.getProperties().get(s).get("range"))) {
				props.add(s);
			}
		}

		System.out.println(props);

/*
		for (DBpediaOntologyNode n : o.nodes) {
            System.out.println(n.className);
            for (HashMap<String, String> p : n.properties) {
                System.out.println(p.get("name"));
            }
            System.out.println();
            System.out.println();
        }
*/
	}

}
