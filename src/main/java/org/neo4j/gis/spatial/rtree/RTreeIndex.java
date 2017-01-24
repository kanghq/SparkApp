/**
 * Copyright (c) 2002-2013 "Neo Technology," Network Engine for Objects in Lund
 * AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.rtree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.gis.spatial.rtree.filter.SearchResults;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

/**
 *
 */
public class RTreeIndex implements SpatialIndexWriter {

	public static final String INDEX_PROP_BBOX = "bbox";

	// Constructor
	public RTreeIndex(GraphDatabaseService database, Node rootNode, EnvelopeDecoder envelopeEncoder) {
		this(database, rootNode, envelopeEncoder, 100);
	}

	public RTreeIndex(GraphDatabaseService database, Node rootNode, EnvelopeDecoder envelopeDecoder, int maxNodeReferences) {
		this.database = database;
		this.rootNode = rootNode;
		this.envelopeDecoder = envelopeDecoder;
		this.maxNodeReferences = maxNodeReferences;

		if (envelopeDecoder == null) {
			throw new NullPointerException("envelopeDecoder is NULL");
		}

		initIndexRoot();
		initIndexMetadata();
	}

	// Public methods
	@Override
	public EnvelopeDecoder getEnvelopeDecoder() {
		return this.envelopeDecoder;
	}

	@Override
	public void add(Node geomNode) {
		// initialize the search with root
		Node parent = getIndexRoot();

		// choose a path down to a leaf
		while (!nodeIsLeaf(parent)) {
			parent = chooseSubTree(parent, geomNode);
		}

		if (countChildren(parent, RTreeRelationshipTypes.RTREE_REFERENCE) >= maxNodeReferences) {
			insertInLeaf(parent, geomNode);
			splitAndAdjustPathBoundingBox(parent);
		} else {
			if (insertInLeaf(parent, geomNode)) {
				// bbox enlargement needed
				adjustPathBoundingBox(parent);
			}
		}

		countSaved = false;
		totalGeometryCount++;
	}

	@Override
	public void remove(long geomNodeId, boolean deleteGeomNode) {
		remove(geomNodeId, deleteGeomNode, true);
	}

	public void remove(long geomNodeId, boolean deleteGeomNode, boolean throwExceptionIfNotFound) {
		
		Node geomNode = null;
		// getNodeById throws NotFoundException if node is already removed
		try {
			geomNode = database.getNodeById(geomNodeId);
			
		} catch (NotFoundException nfe) {
			
			// propagate exception only if flag is set
			if (throwExceptionIfNotFound) {
				throw nfe;
			}
		}
		
		if (geomNode == null && !throwExceptionIfNotFound) {
			//fail silently
			return;
		}

		// be sure geomNode is inside this RTree
		Node indexNode = findLeafContainingGeometryNode(geomNode, throwExceptionIfNotFound);
		if (indexNode == null) {
			return;
		}

		// remove the entry 
		final Relationship geometryRtreeReference = geomNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING);
		if (geometryRtreeReference != null) {
			geometryRtreeReference.delete();
		}
		if (deleteGeomNode) {
			deleteNode(geomNode);
		}

		// reorganize the tree if needed
		if (countChildren(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE) == 0) {
			indexNode = deleteEmptyTreeNodes(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE);
			adjustParentBoundingBox(indexNode, RTreeRelationshipTypes.RTREE_CHILD);
		} else {
			adjustParentBoundingBox(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE);
		}

		adjustPathBoundingBox(indexNode);

		countSaved = false;
		totalGeometryCount--;
	}

	private Node deleteEmptyTreeNodes(Node indexNode, RelationshipType relType) {
		if (countChildren(indexNode, relType) == 0) {
			Node parent = getIndexNodeParent(indexNode);
			if (parent != null) {
				indexNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING).delete();
				indexNode.delete();
				return deleteEmptyTreeNodes(parent, RTreeRelationshipTypes.RTREE_CHILD);
			} else {
				// root
				return indexNode;
			}
		} else {
			return indexNode;
		}
	}

	@Override
	public void removeAll(final boolean deleteGeomNodes, final Listener monitor) {
		Node indexRoot = getIndexRoot();

		monitor.begin(count());
		try {
			// delete all geometry nodes
			visitInTx(new SpatialIndexVisitor() {
				public boolean needsToVisit(Envelope indexNodeEnvelope) {
					return true;
				}

				public void onIndexReference(Node geomNode) {
					geomNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING).delete();
					if (deleteGeomNodes) {
						deleteNode(geomNode);
					}

					monitor.worked(1);
				}
			}, indexRoot.getId());
		} finally {
			monitor.done();
		}

		try (Transaction tx = database.beginTx()) {
			// delete index root relationship
			indexRoot.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.INCOMING).delete();

			// delete tree
			deleteRecursivelySubtree(indexRoot);

			// delete tree metadata
			Relationship metadataNodeRelationship = getRootNode().getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING);
			Node metadataNode = metadataNodeRelationship.getEndNode();
			metadataNodeRelationship.delete();
			metadataNode.delete();

			tx.success();
		}

		countSaved = false;
		totalGeometryCount = 0;
	}

	@Override
	public void clear(final Listener monitor) {
		try (Transaction tx = database.beginTx()) {
			removeAll(false, new NullListener());
			initIndexRoot();
			initIndexMetadata();
			tx.success();
		}
	}

	@Override
	public Envelope getBoundingBox() {
		try (Transaction tx = database.beginTx()) {
			Envelope result = getIndexNodeEnvelope(getIndexRoot());
			tx.success();
			return result;
		}
	}

	@Override
	public int count() {
		saveCount();
		return totalGeometryCount;
	}

	@Override
	public boolean isEmpty() {
		Node indexRoot = getIndexRoot();
		return !indexRoot.hasProperty(INDEX_PROP_BBOX);
	}

	@Override
	public boolean isNodeIndexed(Long geomNodeId) {
		Node geomNode = database.getNodeById(geomNodeId);
		// be sure geomNode is inside this RTree
		return findLeafContainingGeometryNode(geomNode, false) != null;
	}

	public void warmUp() {
		visit(new WarmUpVisitor(), getIndexRoot());
	}

	public Iterable<Node> getAllIndexInternalNodes()
	{
		TraversalDescription td = database.traversalDescription()
				.breadthFirst()
				.relationships( RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING )
				.evaluator( Evaluators.excludeStartPosition() );
        return td.traverse( getIndexRoot() ).nodes();
	}

	@Override
	public Iterable<Node> getAllIndexedNodes() {
		return new IndexNodeToGeometryNodeIterable(getAllIndexInternalNodes());
	}

	private class SearchEvaluator implements Evaluator
	{
		private SearchFilter filter;

		public SearchEvaluator(SearchFilter filter) {
			this.filter = filter;
		}

		@Override
        public Evaluation evaluate( Path path )
        {
            Relationship rel = path.lastRelationship();
            Node node = path.endNode();
            if ( rel == null )
            {
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }
            else if ( rel.isType( RTreeRelationshipTypes.RTREE_CHILD ) )
            {
                return filter.needsToVisit( getIndexNodeEnvelope( node ) ) ?
                       Evaluation.EXCLUDE_AND_CONTINUE :
                       Evaluation.EXCLUDE_AND_PRUNE;
            }
            else if ( rel.isType( RTreeRelationshipTypes.RTREE_REFERENCE ) )
            {
                return filter.geometryMatches( node ) ?
                       Evaluation.INCLUDE_AND_PRUNE :
                       Evaluation.EXCLUDE_AND_PRUNE;
            }
            return null;
        }
    }

	public SearchResults searchIndex(SearchFilter filter) {
		try (Transaction tx = database.beginTx()) {
			SearchEvaluator searchEvaluator = new SearchEvaluator(filter);
			TraversalDescription td = database.traversalDescription()
					.depthFirst()
					.relationships( RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING )
					.relationships( RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING )
					.evaluator( searchEvaluator );
            Traverser traverser = td.traverse( getIndexRoot() );
            SearchResults results = new SearchResults( traverser.nodes() );
            tx.success();
            return results;
		}
	}

	public void visit(SpatialIndexVisitor visitor, Node indexNode) {
		if (!visitor.needsToVisit(getIndexNodeEnvelope(indexNode))) {
			return;
		}

		try (Transaction tx = database.beginTx()) {
			if (indexNode.hasRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
				// Node is not a leaf
				for (Relationship rel : indexNode.getRelationships(RTreeRelationshipTypes.RTREE_CHILD,
					Direction.OUTGOING)) {
					Node child = rel.getEndNode();
					// collect children results
					visit(visitor, child);
				}
			} else if (indexNode.hasRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING)) {
				// Node is a leaf
				for (Relationship rel : indexNode.getRelationships(RTreeRelationshipTypes.RTREE_REFERENCE,
					Direction.OUTGOING)) {
					visitor.onIndexReference(rel.getEndNode());
				}
			}
			tx.success();
		}
	}

	public Node getIndexRoot() {
		try (Transaction tx = database.beginTx()) {
			Node indexRoot = getRootNode().getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.OUTGOING)
				.getEndNode();
			tx.success();
			return indexRoot;
		}
	}

	// Private methods
	private Envelope getChildNodeEnvelope(Node child, RelationshipType relType) {
		if (relType.name().equals(RTreeRelationshipTypes.RTREE_REFERENCE.name())) {
			return getLeafNodeEnvelope(child);
		} else {
			return getIndexNodeEnvelope(child);
		}
	}

	/**
	 * The leaf nodes belong to the domain model, and as such need to use
	 * the layers domain-specific GeometryEncoder for decoding the envelope.
	 */
	private Envelope getLeafNodeEnvelope(Node geomNode) {
		return envelopeDecoder.decodeEnvelope(geomNode);
	}

	/**
	 * The index nodes do NOT belong to the domain model, and as such need
	 * to use the indexes internal knowledge of the index tree and node
	 * structure for decoding the envelope.
	 */
	protected Envelope getIndexNodeEnvelope(Node indexNode) {
		if (indexNode == null) {
			indexNode = getIndexRoot();
		}
		try (Transaction tx = database.beginTx()) {
			if (!indexNode.hasProperty(INDEX_PROP_BBOX)) {
				// this is ok after an index node split
				tx.success();
				return null;
			}

			double[] bbox = (double[]) indexNode.getProperty(INDEX_PROP_BBOX);
			tx.success();
			// Envelope parameters: xmin, xmax, ymin, ymax
			return new Envelope(bbox[0], bbox[2], bbox[1], bbox[3]);
		}
	}

	private void visitInTx(SpatialIndexVisitor visitor, Long indexNodeId) {
		Node indexNode = database.getNodeById(indexNodeId);
		if (!visitor.needsToVisit(getIndexNodeEnvelope(indexNode))) {
			return;
		}

		if (indexNode.hasRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
			// Node is not a leaf

			// collect children
			List<Long> children = new ArrayList<Long>();
			for (Relationship rel : indexNode.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
				children.add(rel.getEndNode().getId());
			}

			// visit children
			for (Long child : children) {
				visitInTx(visitor, child);
			}
		} else if (indexNode.hasRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING)) {
			// Node is a leaf
			try (Transaction tx = database.beginTx()) {
				for (Relationship rel : indexNode.getRelationships(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING)) {
					visitor.onIndexReference(rel.getEndNode());
				}

				tx.success();
			}
		}
	}

	private void initIndexMetadata() {
		Node layerNode = getRootNode();
		if (layerNode.hasRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING)) {
			// metadata already present
			metadataNode = layerNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING).getEndNode();

			maxNodeReferences = (Integer) metadataNode.getProperty("maxNodeReferences");
		} else {
			// metadata initialization
			metadataNode = database.createNode();
			layerNode.createRelationshipTo(metadataNode, RTreeRelationshipTypes.RTREE_METADATA);

			metadataNode.setProperty("maxNodeReferences", maxNodeReferences);
		}

		saveCount();
	}

	private void initIndexRoot() {
		Node layerNode = getRootNode();
		if (!layerNode.hasRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.OUTGOING)) {
			// index initialization
			Node root = database.createNode();
			layerNode.createRelationshipTo(root, RTreeRelationshipTypes.RTREE_ROOT);
		}
	}

	private Node getMetadataNode() {
		if (metadataNode == null) {
			metadataNode = getRootNode().getSingleRelationship(RTreeRelationshipTypes.RTREE_METADATA, Direction.OUTGOING).getEndNode();
		}

		return metadataNode;
	}

	/**
	 * Save the geometry count to the database if it has not been saved yet.
	 * However, if the count is zero, first do an exhaustive search of the
	 * tree and count everything before saving it.
	 */
	private void saveCount() {
		if (totalGeometryCount == 0) {
			SpatialIndexRecordCounter counter = new SpatialIndexRecordCounter();
			visit(counter, getIndexRoot());
			totalGeometryCount = counter.getResult();

			int savedGeometryCount = (int)getMetadataNode().getProperty("totalGeometryCount",0);
			countSaved = savedGeometryCount == totalGeometryCount;
		}

		if (!countSaved) {
			try (Transaction tx = database.beginTx()) {
				getMetadataNode().setProperty("totalGeometryCount", totalGeometryCount);
				countSaved = true;
				tx.success();
			}
		}
	}

	private boolean nodeIsLeaf(Node node) {
		return !node.hasRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
	}

	private Node chooseSubTree(Node parentIndexNode, Node geomRootNode) {
		// children that can contain the new geometry
		List<Node> indexNodes = new ArrayList<Node>();

		// pick the child that contains the new geometry bounding box		
		Iterable<Relationship> relationships = parentIndexNode.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
		for (Relationship relation : relationships) {
			Node indexNode = relation.getEndNode();
			if (getIndexNodeEnvelope(indexNode).contains(getLeafNodeEnvelope(geomRootNode))) {
				indexNodes.add(indexNode);
			}
		}

		if (indexNodes.size() > 1) {
			return chooseIndexNodeWithSmallestArea(indexNodes);
		} else if (indexNodes.size() == 1) {
			return indexNodes.get(0);
		}

		// pick the child that needs the minimum enlargement to include the new geometry
		double minimumEnlargement = Double.POSITIVE_INFINITY;
		relationships = parentIndexNode.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
		for (Relationship relation : relationships) {
			Node indexNode = relation.getEndNode();
			double enlargementNeeded = getAreaEnlargement(indexNode, geomRootNode);

			if (enlargementNeeded < minimumEnlargement) {
				indexNodes.clear();
				indexNodes.add(indexNode);
				minimumEnlargement = enlargementNeeded;
			} else if (enlargementNeeded == minimumEnlargement) {
				indexNodes.add(indexNode);
			}
		}

		if (indexNodes.size() > 1) {
			return chooseIndexNodeWithSmallestArea(indexNodes);
		} else if (indexNodes.size() == 1) {
			return indexNodes.get(0);
		} else {
			// this shouldn't happen
			throw new RuntimeException("No IndexNode found for new geometry");
		}
	}

	private double getAreaEnlargement(Node indexNode, Node geomRootNode) {
		Envelope before = getIndexNodeEnvelope(indexNode);

		Envelope after = getLeafNodeEnvelope(geomRootNode);
		after.expandToInclude(before);

		return getArea(after) - getArea(before);
	}

	private Node chooseIndexNodeWithSmallestArea(List<Node> indexNodes) {
		Node result = null;
		double smallestArea = -1;

		for (Node indexNode : indexNodes) {
			double area = getArea(getIndexNodeEnvelope(indexNode));
			if (result == null || area < smallestArea) {
				result = indexNode;
				smallestArea = area;
			}
		}

		return result;
	}

	private int countChildren(Node indexNode, RelationshipType relationshipType) {
		int counter = 0;
		Iterator<Relationship> iterator = indexNode.getRelationships(relationshipType, Direction.OUTGOING).iterator();
		while (iterator.hasNext()) {
			iterator.next();
			counter++;
		}
		return counter;
	}

	/**
	 * @return is enlargement needed?
	 */
	private boolean insertInLeaf(Node indexNode, Node geomRootNode) {
		return addChild(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE, geomRootNode);
	}

	private void splitAndAdjustPathBoundingBox(Node indexNode) {
		// create a new node and distribute the entries
		Node newIndexNode = quadraticSplit(indexNode);
		Node parent = getIndexNodeParent(indexNode);
		if (parent == null) {
			// if indexNode is the root
			createNewRoot(indexNode, newIndexNode);
		} else {
			expandParentBoundingBoxAfterNewChild(parent, (double[]) indexNode.getProperty(INDEX_PROP_BBOX));

			addChild(parent, RTreeRelationshipTypes.RTREE_CHILD, newIndexNode);

			if (countChildren(parent, RTreeRelationshipTypes.RTREE_CHILD) > maxNodeReferences) {
				splitAndAdjustPathBoundingBox(parent);
			} else {
				adjustPathBoundingBox(parent);
			}
		}
	}

	private Node quadraticSplit(Node indexNode) {
		if (nodeIsLeaf(indexNode)) {
			return quadraticSplit(indexNode, RTreeRelationshipTypes.RTREE_REFERENCE);
		} else {
			return quadraticSplit(indexNode, RTreeRelationshipTypes.RTREE_CHILD);
		}
	}

	private Node quadraticSplit(Node indexNode, RelationshipType relationshipType) {
		List<Node> entries = new ArrayList<Node>();

		Iterable<Relationship> relationships = indexNode.getRelationships(relationshipType, Direction.OUTGOING);
		for (Relationship relationship : relationships) {
			entries.add(relationship.getEndNode());
			relationship.delete();
		}

		// pick two seed entries such that the dead space is maximal
		Node seed1 = null;
		Node seed2 = null;
		double worst = Double.NEGATIVE_INFINITY;
		for (int i = 0; i < entries.size(); ++i) {
			Node e = entries.get(i);
			Envelope eEnvelope = getChildNodeEnvelope(e, relationshipType);
			for (int j = i + 1; j < entries.size(); ++j) {
				Node e1 = entries.get(j);
				Envelope e1Envelope = getChildNodeEnvelope(e1, relationshipType);
				double deadSpace = getArea(createEnvelope(eEnvelope, e1Envelope)) - getArea(eEnvelope) - getArea(e1Envelope);
				if (deadSpace > worst) {
					worst = deadSpace;
					seed1 = e;
					seed2 = e1;
				}
			}
		}

		List<Node> group1 = new ArrayList<Node>();
		group1.add(seed1);
		Envelope group1envelope = getChildNodeEnvelope(seed1, relationshipType);

		List<Node> group2 = new ArrayList<Node>();
		group2.add(seed2);
		Envelope group2envelope = getChildNodeEnvelope(seed2, relationshipType);

		entries.remove(seed1);
		entries.remove(seed2);
		while (entries.size() > 0) {
			// compute the cost of inserting each entry
			List<Node> bestGroup = null;
			Envelope bestGroupEnvelope = null;
			Node bestEntry = null;
			double expansionMin = Double.POSITIVE_INFINITY;
			for (Node e : entries) {
				Envelope nodeEnvelope = getChildNodeEnvelope(e, relationshipType);
				double expansion1 = getArea(createEnvelope(nodeEnvelope, group1envelope)) - getArea(group1envelope);
				double expansion2 = getArea(createEnvelope(nodeEnvelope, group2envelope)) - getArea(group2envelope);

				if (expansion1 < expansion2 && expansion1 < expansionMin) {
					bestGroup = group1;
					bestGroupEnvelope = group1envelope;
					bestEntry = e;
					expansionMin = expansion1;
				} else if (expansion2 < expansion1 && expansion2 < expansionMin) {
					bestGroup = group2;
					bestGroupEnvelope = group2envelope;
					bestEntry = e;
					expansionMin = expansion2;
				} else if (expansion1 == expansion2 && expansion1 < expansionMin) {
					// in case of equality choose the group with the smallest area
					if (getArea(group1envelope) < getArea(group2envelope)) {
						bestGroup = group1;
						bestGroupEnvelope = group1envelope;
					} else {
						bestGroup = group2;
						bestGroupEnvelope = group2envelope;
					}
					bestEntry = e;
					expansionMin = expansion1;
				}
			}

			// insert the best candidate entry in the best group
			bestGroup.add(bestEntry);
			bestGroupEnvelope.expandToInclude(getChildNodeEnvelope(bestEntry, relationshipType));

			entries.remove(bestEntry);
		}

		// reset bounding box and add new children
		indexNode.removeProperty(INDEX_PROP_BBOX);
		for (Node node : group1) {
			addChild(indexNode, relationshipType, node);
		}

		// create new node from split
		Node newIndexNode = database.createNode();
		for (Node node : group2) {
			addChild(newIndexNode, relationshipType, node);
		}

		return newIndexNode;
	}

	private void createNewRoot(Node oldRoot, Node newIndexNode) {
		Node newRoot = database.createNode();
		addChild(newRoot, RTreeRelationshipTypes.RTREE_CHILD, oldRoot);
		addChild(newRoot, RTreeRelationshipTypes.RTREE_CHILD, newIndexNode);

		Node layerNode = getRootNode();
		layerNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.OUTGOING).delete();
		layerNode.createRelationshipTo(newRoot, RTreeRelationshipTypes.RTREE_ROOT);
	}

	private boolean addChild(Node parent, RelationshipType type, Node newChild) {
		Envelope childEnvelope = getChildNodeEnvelope(newChild, type);
		double[] childBBox = new double[]{
			childEnvelope.getMinX(), childEnvelope.getMinY(),
			childEnvelope.getMaxX(), childEnvelope.getMaxY()};
		parent.createRelationshipTo(newChild, type);
		return expandParentBoundingBoxAfterNewChild(parent, childBBox);
	}

	private void adjustPathBoundingBox(Node indexNode) {
		Node parent = getIndexNodeParent(indexNode);
		if (parent != null) {
			if (adjustParentBoundingBox(parent, RTreeRelationshipTypes.RTREE_CHILD)) {
				// entry has been modified: adjust the path for the parent
				adjustPathBoundingBox(parent);
			}
		}
	}

	/**
	 * Fix an IndexNode bounding box after a child has been removed
	 *
	 * @param indexNode
	 * @return true if something has changed
	 */
	private boolean adjustParentBoundingBox(Node indexNode, RelationshipType relationshipType) {
		double[] old = null;
		if (indexNode.hasProperty(INDEX_PROP_BBOX)) {
			old = (double[]) indexNode.getProperty(INDEX_PROP_BBOX);
		}

		Envelope bbox = null;

		Iterator<Relationship> iterator = indexNode.getRelationships(relationshipType, Direction.OUTGOING).iterator();
		while (iterator.hasNext()) {
			Node childNode = iterator.next().getEndNode();

			if (bbox == null) {
				bbox = new Envelope(getChildNodeEnvelope(childNode, relationshipType));
			} else {
				bbox.expandToInclude(getChildNodeEnvelope(childNode, relationshipType));
			}
		}

		if (bbox == null) {
			// this could happen in an empty tree
			bbox = new Envelope(0, 0, 0, 0);
		}

		if (old.length != 4
			|| bbox.getMinX() != old[0]
			|| bbox.getMinY() != old[1]
			|| bbox.getMaxX() != old[2]
			|| bbox.getMaxY() != old[3]) {
			indexNode.setProperty(INDEX_PROP_BBOX, new double[]{bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY()});
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Adjust IndexNode bounding box according to the new child inserted
	 *
	 * @param parent IndexNode
	 * @param childBBox geomNode inserted
	 * @return is bbox changed?
	 */
	private boolean expandParentBoundingBoxAfterNewChild(Node parent, double[] childBBox) {
		if (!parent.hasProperty(INDEX_PROP_BBOX)) {
			parent.setProperty(INDEX_PROP_BBOX, new double[]{childBBox[0], childBBox[1], childBBox[2], childBBox[3]});
			return true;
		}

		double[] parentBBox = (double[]) parent.getProperty(INDEX_PROP_BBOX);

		boolean valueChanged = setMin(parentBBox, childBBox, 0);
		valueChanged = setMin(parentBBox, childBBox, 1) || valueChanged;
		valueChanged = setMax(parentBBox, childBBox, 2) || valueChanged;
		valueChanged = setMax(parentBBox, childBBox, 3) || valueChanged;

		if (valueChanged) {
			parent.setProperty(INDEX_PROP_BBOX, parentBBox);
		}

		return valueChanged;
	}

	private boolean setMin(double[] parent, double[] child, int index) {
		if (parent[index] > child[index]) {
			parent[index] = child[index];
			return true;
		} else {
			return false;
		}
	}

	private boolean setMax(double[] parent, double[] child, int index) {
		if (parent[index] < child[index]) {
			parent[index] = child[index];
			return true;
		} else {
			return false;
		}
	}

	private Node getIndexNodeParent(Node indexNode) {
		Relationship relationship = indexNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
		if (relationship == null) {
			return null;
		} else {
			return relationship.getStartNode();
		}
	}

	private double getArea(Envelope e) {
		return e.getWidth() * e.getHeight();
	}

	private void deleteRecursivelySubtree(Node indexNode) {
		for (Relationship relationship : indexNode.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING)) {
			deleteRecursivelySubtree(relationship.getEndNode());
		}

		Relationship relationshipWithFather = indexNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
		// the following check is needed because rootNode doesn't have this relationship
		if (relationshipWithFather != null) {
			relationshipWithFather.delete();
		}

		indexNode.delete();
	}

	protected Node findLeafContainingGeometryNode(Node geomNode, boolean throwExceptionIfNotFound) {
		if (!geomNode.hasRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING)) {
			if (throwExceptionIfNotFound) {
				throw new RuntimeException("GeometryNode not indexed with an RTree: " + geomNode.getId());
			} else {
				return null;
			}
		}

		Node indexNodeLeaf = geomNode.getSingleRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING).getStartNode();

		Node root = null;
		Node child = indexNodeLeaf;
		while (root == null) {
			Node parent = getIndexNodeParent(child);
			if (parent == null) {
				root = child;
			} else {
				child = parent;
			}
		}

		if (root.getId() != getIndexRoot().getId()) {
			if (throwExceptionIfNotFound) {
				throw new RuntimeException("GeometryNode not indexed in this RTree: " + geomNode.getId());
			} else {
				return null;
			}
		} else {
			return indexNodeLeaf;
		}
	}

	private void deleteNode(Node node) {
		for (Relationship r : node.getRelationships()) {
			r.delete();
		}
		node.delete();
	}

	private Node getRootNode() {
		return rootNode;
	}

	/**
	 * Create a bounding box encompassing the two bounding boxes passed in.
	 */
	private static Envelope createEnvelope(Envelope e, Envelope e1) {
		Envelope result = new Envelope(e);
		result.expandToInclude(e1);
		return result;
	}

	// Attributes
	public GraphDatabaseService getDatabase() {
		return database;
	}

	private GraphDatabaseService database;

	private Node rootNode;
	private EnvelopeDecoder envelopeDecoder;
	private int maxNodeReferences;

	private Node metadataNode;
	private int totalGeometryCount = 0;
	private boolean countSaved = false;

	// Private classes
	private class WarmUpVisitor implements SpatialIndexVisitor {

		public boolean needsToVisit(Envelope indexNodeEnvelope) {
			return true;
		}

		public void onIndexReference(Node geomNode) {
		}
	}

	/**
	 * In order to wrap one iterable or iterator in another that converts
	 * the objects from one type to another without loading all into memory,
	 * we need to use this ugly java-magic. Man, I miss Ruby right now!
	 *
	 * @author Craig
	 */
	private class IndexNodeToGeometryNodeIterable implements Iterable<Node> {

		private Iterator<Node> allIndexNodeIterator;

		private class GeometryNodeIterator implements Iterator<Node> {

			Iterator<Node> geometryNodeIterator = null;

			public boolean hasNext() {
				checkGeometryNodeIterator();
				return geometryNodeIterator != null && geometryNodeIterator.hasNext();
			}

			public Node next() {
				checkGeometryNodeIterator();
				return geometryNodeIterator == null ? null : geometryNodeIterator.next();
			}

            private void checkGeometryNodeIterator()
            {
                TraversalDescription td = database.traversalDescription()
                        .depthFirst()
                        .relationships( RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING )
                        .evaluator( Evaluators.excludeStartPosition() )
                        .evaluator( Evaluators.toDepth( 1 ) );
                while ( (geometryNodeIterator == null || !geometryNodeIterator.hasNext()) &&
                        allIndexNodeIterator.hasNext() )
                {
                    geometryNodeIterator = td.traverse( allIndexNodeIterator.next() ).nodes().iterator();
                }
            }

			public void remove() {
			}
		}

		public IndexNodeToGeometryNodeIterable(Iterable<Node> allIndexNodes) {
			this.allIndexNodeIterator = allIndexNodes.iterator();
		}

		public Iterator<Node> iterator() {
			return new GeometryNodeIterator();
		}
	}
}
