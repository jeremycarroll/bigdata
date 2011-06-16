package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Utility Java class for outputting XML.
 * <p>
 * The motivation is to provide a similar interface to a document builder but
 * instead of building an in-memory model, to directly stream the output.
 * <p>
 * Example usage:
 * <pre>
 * 	<xml>
 *    <div attr="attr1">Content</div>
 *    <div attr="attr1"/>
 *    <div>Content</div>
 *    <div>
 *    	<div>Content</div>
 *    </div>
 *  </xml>
 *  
 *  XMLBuilder.Node closed = new XMLBuilder(false)
 *  	.root("xml")
 *  		.node("div")
 *  			.attr("attr", "attr1")
 *  			.text("Content")
 *  			.close()
 *  		.node("div", "Content")
 *  		.node("div")
 *  			.node("div", "Content")
 *  			.close()
 *  		.close();
 *  
 *  // The close on the root node will return null
 *  
 *  assert(closed == null);
 * </pre>
 * 
 * @author Martyn Cutcher
 */
public class XMLBuilder {
    
	private final boolean xml;
	private final Writer m_writer;
	
//	private boolean m_pp = false;
	
	public XMLBuilder() throws IOException {
		this(true, (OutputStream) null);
	}
	
	public XMLBuilder(boolean xml) throws IOException {
		this(xml, (OutputStream) null);
	}
	
	public XMLBuilder(boolean xml, OutputStream outstr) throws IOException {
		this(xml,null/*encoding*/,outstr);
	}
	
	public XMLBuilder(boolean xml, String encoding) throws IOException {

	    this(xml, encoding, (OutputStream) null);
	    
	}
	
	public XMLBuilder(boolean xml, String encoding, OutputStream outstr) throws IOException {
        
		this.xml = xml;
		
	    if (outstr == null) {
            m_writer = new StringWriter();
        } else {
            m_writer = new OutputStreamWriter(outstr);
        }
		
		if (xml) {
			if(encoding!=null) {
				m_writer.write("<?xml version=\"1.0\" encoding=\"" + encoding + "\"?>");
			} else {
				m_writer.write("<?xml version=\"1.0\"?>");
			}
		} else {
			// TODO Note the optional encoding for use in a meta tag.
			m_writer.write("<!DOCTYPE HTML PUBLIC");
			m_writer.write(" \"-//W3C//DTD HTML 4.01 Transitional//EN\"");
			m_writer.write(" \"http://www.w3.org/TR/html4/loose.dtd\">");
		}
		
	}
	
//	public void prettyPrint(boolean pp) {
//		m_pp = pp;
//	}
	
//	private void initWriter(OutputStream outstr) {
//	}

	public String toString() {
		return m_writer.toString();
	}
	
	public Node root(String name) throws IOException {
		return new Node(name, null);
	}
	
	public Node root(String name, String nodeText) throws IOException {
		Node root = new Node(name, null);
		root.text(nodeText);
		
		return root.close();
	}
	
	public void closeAll(Node node) throws IOException {
		while (node != null) {
			node = node.close();
		}
	}
	
	public class Node {
		boolean m_open = true;
		String m_tag;
		Node m_parent;
		int m_attrs = 0;
		int m_text = 0;
		int m_nodes = 0;
		
		Node(String name, Node parent) throws IOException {
			m_tag = name;
			m_parent = parent;
			
			m_writer.write("<" + m_tag);
		}
		
		public Node attr(String attr, Object value) throws IOException {
			m_writer.write(" " + attr + "=\"" + value + "\"");
			m_attrs++;
			
			return this;
		}
		
		public Node text(String text) throws IOException {
			closeHead();
			
			m_writer.write(text);
			m_text++;
			
			return this;
		}
		
		public Node node(String tag) throws IOException {
			closeHead();
			
			m_nodes++;
			
			return new Node(tag, this);
		}
		
		public Node node(String tag, String text) throws IOException {
			closeHead();
			
			m_nodes++;
			
			Node tmp = new Node(tag, this);
			tmp.text(text);
			
			return tmp.close();
		}

		/**
		 * Close the open element.
		 * 
		 * @return The parent element.
		 * @throws IOException
		 */
		public Node close() throws IOException {
			return close(!xml);
		}

		/**
		 * Close the open element.
		 * 
		 * @param simpleEnd
		 *            When <code>true</code> an open tag without a body will be
		 *            closed by a single &gt; symbol rather than the XML style
		 *            &#47;&gt;.
		 * 
		 * @return The parent element.
		 * @throws IOException
		 */
		public Node close(final boolean simpleEnd) throws IOException {
			assert(m_open);
			
			if (emptyBody()) {
				if(simpleEnd) {
					m_writer.write(">");
				} else {
					m_writer.write("/>");
				}
			} else {
				m_writer.write("</" + m_tag + "\n>");
			}
			
			m_open = false;
			
			// If root node then flush the writer
			if (m_parent == null) {
				m_writer.flush();
			}
			
			return m_parent;
		}
		
		private void closeHead() throws IOException {
			if (emptyBody()) {
				m_writer.write(">");
			}
		}
		
		private boolean emptyBody() {
			return m_nodes == 0 && m_text == 0;
		}
		
	}

}
