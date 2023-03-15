//------------------------------------
// IndexDesc.java
//
// Ning Wang, April,24  1998
//-------------------------------------

package catalog;

import global.*;

// IndexDesc class: schema for index catalog
public class IndexDesc
{
    	String relName;                     // relation name
	String attrName;                    // attribute name
	IndexType  accessType;                // access method
	MapOrder order;                     // order of keys
	int        clustered = 0;                 //
	int        distinctKeys = 0;              // no of distinct key values
	int        indexPages = 0;                // no of index pages
};

