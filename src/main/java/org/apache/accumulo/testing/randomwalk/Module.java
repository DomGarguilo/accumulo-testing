/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.testing.randomwalk;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * A module is directed graph of tests
 */
public class Module extends Node {

  private static final Logger log = LoggerFactory.getLogger(Module.class);

  private static class Dummy extends Node {

    final String name;

    Dummy(String name) {
      this.name = name;
    }

    @Override
    public void visit(State state, RandWalkEnv env, Properties props) {
      String print;
      if ((print = props.getProperty("print")) != null) {
        switch (print) {
          case "TRACE":
            log.trace(name);
            break;
          case "DEBUG":
            log.debug(name);
            break;
          case "INFO":
            log.info(name);
            break;
          case "WARN":
            log.warn(name);
            break;
          case "ERROR":
            log.error(name);
            break;
          default:
            log.info(name);
            break;
        }
      }
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private class Alias extends Node {

    Node target;
    String targetId;
    final String id;

    Alias(String id) {
      target = null;
      this.id = id;
    }

    @Override
    public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
      throw new Exception("You don't visit aliases!");
    }

    @Override
    public String toString() {
      return id;
    }

    public void update(String node) throws Exception {
      targetId = node;
      target = getNode(node);
    }

    public Node get() {
      return target;
    }

    public String getTargetId() {
      return targetId;
    }
  }

  private final HashMap<String,Node> nodes = new HashMap<>();
  private final HashMap<String,Properties> localProps = new HashMap<>();

  private static class Edge {
    String nodeId;
    int weight;
  }

  private static class AdjList {

    private final List<Edge> edges = new ArrayList<>();
    private int totalWeight = 0;

    /**
     * Adds a neighbor node and weight of edge
     */
    private void addEdge(String nodeId, int weight) {

      totalWeight += weight;

      Edge e = new Edge();
      e.nodeId = nodeId;
      e.weight = weight;
      edges.add(e);
    }

    /**
     * Chooses a random neighbor node
     *
     * @return Node or null if no edges
     */
    private String randomNeighbor() {

      String nodeId = null;
      Random rand = new Random();

      int randNum = rand.nextInt(totalWeight) + 1;
      int sum = 0;

      for (Edge e : edges) {
        nodeId = e.nodeId;
        sum += e.weight;
        if (randNum <= sum) {
          break;
        }
      }
      return nodeId;
    }
  }

  private final HashMap<String,String> prefixes = new HashMap<>();
  private final HashMap<String,AdjList> adjMap = new HashMap<>();
  private final HashMap<String,Set<String>> aliasMap = new HashMap<>();
  private final String id;
  private String initNodeId;
  private Fixture fixture = null;

  public Module(String id) throws Exception {
    this.id = id;
    loadFromXml();
  }

  @Override
  public void visit(final State state, final RandWalkEnv env, Properties props) throws Exception {
    int maxHops, maxSec;
    boolean teardown;

    Properties initProps = getProps("_init");
    initProps.putAll(props);
    String prop;
    if ((prop = initProps.getProperty("maxHops")) == null || prop.equals("0") || prop.equals(""))
      maxHops = Integer.MAX_VALUE;
    else
      maxHops = Integer.parseInt(initProps.getProperty("maxHops", "0"));

    if ((prop = initProps.getProperty("maxSec")) == null || prop.equals("0") || prop.equals(""))
      maxSec = Integer.MAX_VALUE;
    else
      maxSec = Integer.parseInt(initProps.getProperty("maxSec", "0"));

    teardown = (prop = initProps.getProperty("teardown")) == null || prop.equals("true")
        || prop.equals("");

    if (fixture != null) {
      fixture.setUp(state, env);
    }

    ExecutorService service = ThreadPools.getServerThreadPools().getPoolBuilder("RandomWalk Runner")
        .numCoreThreads(1).build();

    try {
      Node initNode = getNode(initNodeId);

      boolean test = false;
      if (initNode instanceof Test) {
        startTimer(initNode);
        test = true;
      }
      initNode.visit(state, env, getProps(initNodeId));
      if (test)
        stopTimer(initNode);

      // update aliases
      Set<String> aliases;
      if ((aliases = aliasMap.get(initNodeId)) != null)
        for (String alias : aliases) {
          ((Alias) nodes.get(alias)).update(initNodeId);
        }

      String curNodeId = initNodeId;
      int numHops = 0;
      long startTime = System.currentTimeMillis() / 1000;
      while (true) {
        // check if END state was reached
        if (curNodeId.equalsIgnoreCase("END")) {
          log.debug("reached END state");
          break;
        }
        // check if maxSec was reached
        long curTime = System.currentTimeMillis() / 1000;
        if ((curTime - startTime) > maxSec) {
          log.debug("reached maxSec(" + maxSec + ")");
          break;
        }

        // The number of seconds before the test should exit
        long secondsRemaining = maxSec - (curTime - startTime);

        // check if maxHops was reached
        if (numHops > maxHops) {
          log.debug("reached maxHops(" + maxHops + ")");
          break;
        }
        numHops++;

        if (!adjMap.containsKey(curNodeId) && !curNodeId.startsWith("alias.")) {
          throw new Exception(
              "Reached node(" + curNodeId + ") without outgoing edges in module(" + this + ")");
        }
        AdjList adj = adjMap.get(curNodeId);
        String nextNodeId = adj.randomNeighbor();
        final Node nextNode;
        Node nextNodeOrAlias = getNode(nextNodeId);
        if (nextNodeOrAlias instanceof Alias) {
          nextNodeId = ((Alias) nextNodeOrAlias).getTargetId();
          nextNode = ((Alias) nextNodeOrAlias).get();
        } else {
          nextNode = nextNodeOrAlias;
        }
        final Properties nodeProps = getProps(nextNodeId);
        try {
          test = false;
          if (nextNode instanceof Test) {
            startTimer(nextNode);
            test = true;
          }

          // Wrap the visit of the next node in the module in a
          // callable that returns a thrown exception
          FutureTask<Exception> task = new FutureTask<>(() -> {
            try {
              nextNode.visit(state, env, nodeProps);
              return null;
            } catch (Exception e) {
              return e;
            }
          });

          // Run the task (should execute immediately)
          service.submit(task);

          Exception nodeException;
          try {
            // Bound the time we'll wait for the node to complete
            nodeException = task.get(secondsRemaining, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            log.warn("Interrupted waiting for " + nextNode.getClass().getSimpleName()
                + " to complete. Exiting.", e);
            break;
          } catch (ExecutionException e) {
            log.error("Caught error executing " + nextNode.getClass().getSimpleName(), e);
            throw e;
          } catch (TimeoutException e) {
            log.info("Timed out waiting for " + nextNode.getClass().getSimpleName()
                + " to complete (waited " + secondsRemaining + " seconds). Exiting.", e);
            break;
          }

          // The RandomWalk node throw an Exception that that Callable
          // handed back
          // Throw it and let the Module perform cleanup
          if (null != nodeException) {
            throw nodeException;
          }

          if (test)
            stopTimer(nextNode);
        } catch (Exception e) {
          log.debug("AccumuloClient belongs to user: " + env.getAccumuloClient().whoami());
          log.debug("Exception occured at: " + System.currentTimeMillis());
          log.debug("Properties for node: " + nextNodeId);
          for (Entry<Object,Object> entry : nodeProps.entrySet()) {
            log.debug("  " + entry.getKey() + ": " + entry.getValue());
          }
          log.debug("Overall Configuration Properties");
          for (Entry<Object,Object> entry : env.getTestProperties().entrySet()) {
            log.debug("  " + entry.getKey() + ": " + entry.getValue());
          }
          log.debug("State information");
          for (String key : new TreeSet<>(state.getMap().keySet())) {
            Object value = state.getMap().get(key);
            String logMsg = "  " + key + ": ";
            if (value == null)
              logMsg += "null";
            else if (value instanceof String || value instanceof Map || value instanceof Collection
                || value instanceof Number)
              logMsg += value;
            else if (value instanceof byte[])
              logMsg += new String((byte[]) value, UTF_8);
            else if (value instanceof PasswordToken)
              logMsg += new String(((PasswordToken) value).getPassword(), UTF_8);
            else
              logMsg += value.getClass() + " - " + value;

            log.debug(logMsg);
          }
          throw new Exception("Error running node " + nextNodeId, e);
        }

        // update aliases
        if ((aliases = aliasMap.get(curNodeId)) != null)
          for (String alias : aliases) {
            ((Alias) nodes.get(alias)).update(curNodeId);
          }

        curNodeId = nextNodeId;
      }
    } finally {
      if (null != service) {
        service.shutdownNow();
      }
    }

    if (teardown && (fixture != null)) {
      log.debug("tearing down module");
      fixture.tearDown(state, env);
    }
  }

  Thread timer = null;
  final int time = 5 * 1000 * 60;
  final AtomicBoolean runningLong = new AtomicBoolean(false);
  long systemTime;

  /**
   *
   */
  private void startTimer(final Node initNode) {
    runningLong.set(false);
    timer = new Thread(() -> {
      try {
        systemTime = System.currentTimeMillis();
        Thread.sleep(time);
      } catch (InterruptedException ie) {
        return;
      }
      long timeSinceLastProgress = System.currentTimeMillis() - initNode.lastProgress();
      if (timeSinceLastProgress > time) {
        log.warn("Node " + initNode + " has been running for " + timeSinceLastProgress / 1000.0
            + " seconds. You may want to look into it.");
        runningLong.set(true);
      }
    });
    initNode.makingProgress();
    timer.start();
  }

  /**
   *
   */
  private void stopTimer(Node nextNode) {
    synchronized (timer) {
      timer.interrupt();
      try {
        timer.join();
      } catch (InterruptedException e) {
        log.error("Failed to join timer '" + timer.getName() + "'.", e);
      }
    }
    if (runningLong.get())
      log.warn("Node " + nextNode + ", which was running long, has now completed after "
          + (System.currentTimeMillis() - systemTime) / 1000.0 + " seconds");
  }

  @Override
  public String toString() {
    return id;
  }

  private String getFullName(String name) {

    int index = name.indexOf(".");
    if (index == -1 || name.endsWith(".xml")) {
      return name;
    }

    String id = name.substring(0, index);

    if (!prefixes.containsKey(id)) {
      log.warn("Id (" + id + ") was not found in prefixes");
      return name;
    }

    return prefixes.get(id).concat(name.substring(index + 1));
  }

  private Node createNode(String id, String src) throws Exception {

    // check if id indicates dummy node
    if (id.equalsIgnoreCase("END") || id.startsWith("dummy")) {
      if (!nodes.containsKey(id)) {
        nodes.put(id, new Dummy(id));
      }
      return nodes.get(id);
    }

    if (id.startsWith("alias")) {
      if (!nodes.containsKey(id)) {
        nodes.put(id, new Alias(id));
      }
      return nodes.get(id);
    }

    // grab node from framework based on its id or src
    Node node;
    if (src == null || src.isEmpty()) {
      node = Framework.getInstance().getNode(getFullName(id));
    } else {
      node = Framework.getInstance().getNode(getFullName(src));
    }

    // add to node to this module's map
    nodes.put(id, node);

    return node;
  }

  private Node getNode(String id) throws Exception {

    if (nodes.containsKey(id)) {
      return nodes.get(id);
    }

    if (id.equalsIgnoreCase("END")) {
      nodes.put(id, new Dummy(id));
      return nodes.get(id);
    }

    return Framework.getInstance().getNode(getFullName(id));
  }

  private Properties getProps(String nodeId) {
    if (localProps.containsKey(nodeId)) {
      return localProps.get(nodeId);
    }
    return new Properties();
  }

  private void loadFromXml() throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder docbuilder;
    Document d;

    // set the schema
    SchemaFactory sf = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
    Schema moduleSchema =
        sf.newSchema(this.getClass().getClassLoader().getResource("randomwalk/module.xsd"));
    dbf.setSchema(moduleSchema);

    // parse the document

    try {
      docbuilder = dbf.newDocumentBuilder();
      d = docbuilder.parse(this.getClass().getResourceAsStream("/randomwalk/modules/" + id));
    } catch (Exception e) {
      log.error("Failed to parse xml at randomwalk/modules/" + id, e);
      throw new Exception("Failed to parse xml at randomwalk/modules/" + id);
    }

    // parse packages
    NodeList nodelist = d.getDocumentElement().getElementsByTagName("package");
    for (int i = 0; i < nodelist.getLength(); i++) {
      Element el = (Element) nodelist.item(i);
      String value = el.getAttribute("value");
      if (!value.endsWith(".")) {
        value = value.concat(".");
      }
      prefixes.put(el.getAttribute("prefix"), value);
    }

    // parse fixture node
    nodelist = d.getDocumentElement().getElementsByTagName("fixture");
    if (nodelist.getLength() > 0) {
      Element fixtureEl = (Element) nodelist.item(0);
      fixture = (Fixture) Class.forName(getFullName(fixtureEl.getAttribute("id")))
          .getDeclaredConstructor().newInstance();
    }

    // parse initial node
    Element initEl = (Element) d.getDocumentElement().getElementsByTagName("init").item(0);
    initNodeId = initEl.getAttribute("id");
    Properties initProps = new Properties();
    String attr = initEl.getAttribute("maxHops");

    initProps.setProperty("maxHops", attr);
    attr = initEl.getAttribute("maxSec");

    initProps.setProperty("maxSec", attr);
    attr = initEl.getAttribute("teardown");

    initProps.setProperty("teardown", attr);
    localProps.put("_init", initProps);

    // parse all nodes
    nodelist = d.getDocumentElement().getElementsByTagName("node");
    for (int i = 0; i < nodelist.getLength(); i++) {

      Element nodeEl = (Element) nodelist.item(i);

      // get attributes
      String id = nodeEl.getAttribute("id");
      if (adjMap.containsKey(id)) {
        throw new Exception("Module already contains: " + id);
      }
      String src = nodeEl.getAttribute("src");

      // create node
      createNode(id, src);

      // set some attributes in properties for later use
      Properties props = new Properties();
      props.setProperty("maxHops", nodeEl.getAttribute("maxHops"));
      props.setProperty("maxSec", nodeEl.getAttribute("maxSec"));
      props.setProperty("teardown", nodeEl.getAttribute("teardown"));

      // parse aliases
      NodeList aliaslist = nodeEl.getElementsByTagName("alias");
      Set<String> aliases = new TreeSet<>();
      for (int j = 0; j < aliaslist.getLength(); j++) {
        Element propEl = (Element) aliaslist.item(j);

        if (!propEl.hasAttribute("name")) {
          throw new Exception("Node " + id + " has alias with no identifying name");
        }

        String key = "alias." + propEl.getAttribute("name");

        aliases.add(key);
        createNode(key, null);
      }
      if (aliases.size() > 0)
        aliasMap.put(id, aliases);

      // parse properties of nodes
      NodeList proplist = nodeEl.getElementsByTagName("property");
      for (int j = 0; j < proplist.getLength(); j++) {
        Element propEl = (Element) proplist.item(j);

        if (!propEl.hasAttribute("key") || !propEl.hasAttribute("value")) {
          throw new Exception("Node " + id + " has property with no key or value");
        }

        String key = propEl.getAttribute("key");

        if (key.equals("maxHops") || key.equals("maxSec") || key.equals("teardown")) {
          throw new Exception("The following property can only be set in attributes: " + key);
        }

        props.setProperty(key, propEl.getAttribute("value"));
      }
      localProps.put(id, props);

      // parse edges of nodes
      AdjList edges = new AdjList();
      adjMap.put(id, edges);
      NodeList edgelist = nodeEl.getElementsByTagName("edge");
      if (edgelist.getLength() == 0) {
        throw new Exception("Node " + id + " has no edges!");
      }
      for (int j = 0; j < edgelist.getLength(); j++) {
        Element edgeEl = (Element) edgelist.item(j);

        String edgeID = edgeEl.getAttribute("id");

        if (!edgeEl.hasAttribute("weight")) {
          throw new Exception("Edge with id=" + edgeID + " is missing weight");
        }

        int weight = Integer.parseInt(edgeEl.getAttribute("weight"));
        edges.addEdge(edgeID, weight);
      }
    }
  }
}
