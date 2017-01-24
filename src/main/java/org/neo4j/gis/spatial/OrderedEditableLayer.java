/**
 * Copyright (c) 2010-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial;

import static org.neo4j.gis.spatial.utilities.TraverserFactory.createTraverserInBackwardsCompatibleWay;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchOrderingPolicies;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.shell.kernel.apps.Trav;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This class extends the EditableLayerImpl in a way that allows for the
 * geometry order to be maintained. If the user wishes to iterate through the
 * geometries in the same order they were created, they can use the
 * getAllGeometryNodes method for this. The super-class EditableLayerImpl used
 * to have this behavior, but the cost of maintaining the chain through the REST
 * API was too high, because the previous node could not be easily cached. So we
 * moved it to this class, and made it optional. The Java API should not suffer
 * the performance penalty of this, but we decided to make the default behavior
 * non-ordered for a simpler data structure.
 * 
 * @author craig
 */
public class OrderedEditableLayer extends EditableLayerImpl {
	private Node previousGeomNode;
	enum OrderedRelationshipTypes implements RelationshipType {
		GEOMETRIES, NEXT_GEOM
	}

	protected Node addGeomNode(Geometry geom, String[] fieldsName, Object[] fields) {
		Node geomNode = super.addGeomNode(geom, fieldsName, fields);
		if (previousGeomNode == null) {
			TraversalDescription traversalDescription = getDatabase().traversalDescription()
					.order( BranchOrderingPolicies.POSTORDER_BREADTH_FIRST )
					.relationships(OrderedRelationshipTypes.GEOMETRIES, Direction.INCOMING)
					.relationships(OrderedRelationshipTypes.NEXT_GEOM, Direction.INCOMING)
					.evaluator(Evaluators.excludeStartPosition());
			for (Node node : createTraverserInBackwardsCompatibleWay(traversalDescription, layerNode).nodes()) {
				previousGeomNode = node;
			}
		}
		if (previousGeomNode != null) {
			previousGeomNode.createRelationshipTo(geomNode, OrderedRelationshipTypes.NEXT_GEOM);
		} else {
			layerNode.createRelationshipTo(geomNode, OrderedRelationshipTypes.GEOMETRIES);
		}
		previousGeomNode = geomNode;
		return geomNode;
	}

    /**
     * Provides a method for iterating over all nodes that represent geometries in this dataset.
     * This is similar to the getAllNodes() methods from GraphDatabaseService but will only return
     * nodes that this dataset considers its own, and can be passed to the GeometryEncoder to
     * generate a Geometry. There is no restricting on a node belonging to multiple datasets, or
     * multiple layers within the same dataset.
     * 
     * @return iterable over geometry nodes in the dataset
     */
	public Iterable<Node> getAllGeometryNodes() {
		TraversalDescription td = getDatabase().traversalDescription().depthFirst()
				.evaluator( Evaluators.excludeStartPosition() )
				.relationships( OrderedRelationshipTypes.GEOMETRIES, Direction.OUTGOING )
				.relationships( OrderedRelationshipTypes.NEXT_GEOM, Direction.OUTGOING );
		return td.traverse( layerNode ).nodes();
	}

}
