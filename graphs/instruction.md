Diagrams
Diagram is a primary object representing a diagram.

Basic
Diagram represents a global diagram context.

You can create a diagram context with the Diagram class. The first parameter of the Diagram constructor will be used to generate the output filename.

from diagrams import Diagram
from diagrams.aws.compute import EC2

with Diagram("Simple Diagram"):
EC2("web")
If you run the above script with the command below,

$ python diagram.py
it will generate an image file with single EC2 node drawn as simple_diagram.png in your working directory and open that created image file immediately.

Jupyter Notebooks
Diagrams can also be rendered directly inside Jupyter notebooks like this:

from diagrams import Diagram
from diagrams.aws.compute import EC2

with Diagram("Simple Diagram") as diag:
EC2("web")
diag
Options
You can specify the output file format with the outformat parameter. The default is png.

Allowed formats are: png, jpg, svg, pdf, and dot

from diagrams import Diagram
from diagrams.aws.compute import EC2

with Diagram("Simple Diagram", outformat="jpg"):
EC2("web")
The outformat parameter also supports a list to output all the defined outputs in one call:

from diagrams import Diagram
from diagrams.aws.compute import EC2

with Diagram("Simple Diagram Multi Output", outformat=["jpg", "png", "dot"]):
EC2("web")
You can specify the output filename with the filename parameter. The extension shouldn't be included, it's determined by the outformat parameter.

from diagrams import Diagram
from diagrams.aws.compute import EC2

with Diagram("Simple Diagram", filename="my_diagram"):
EC2("web")
You can also disable the automatic file opening by setting the show parameter to false. The default is true.

from diagrams import Diagram
from diagrams.aws.compute import EC2

with Diagram("Simple Diagram", show=False):
EC2("web")
Diagrams also allow custom Graphviz dot attributes options.

graph_attr, node_attr and edge_attr are supported. Here is a reference link.

from diagrams import Diagram
from diagrams.aws.compute import EC2

graph_attr = {
"fontsize": "45",
"bgcolor": "transparent"
}

with Diagram("Simple Diagram", show=False, graph_attr=graph_attr):
EC2("web")

Nodes
Node is an object representing a node or system component.

Basic
Node is an abstract concept that represents a single system component object.

A node object consists of three parts: provider, resource type and name. You may already have seen each part in the previous example.

from diagrams import Diagram
from diagrams.aws.compute import EC2

with Diagram("Simple Diagram"):
EC2("web")
In the example above, the EC2 is a node of resource type compute which is provided by the aws provider.

You can use other node objects in a similar manner:

# aws resources

from diagrams.aws.compute import ECS, Lambda
from diagrams.aws.database import RDS, ElastiCache
from diagrams.aws.network import ELB, Route53, VPC
...

# azure resources

from diagrams.azure.compute import FunctionApps
from diagrams.azure.storage import BlobStorage
...

# alibaba cloud resources

from diagrams.alibabacloud.compute import ECS
from diagrams.alibabacloud.storage import ObjectTableStore
...

# gcp resources

from diagrams.gcp.compute import AppEngine, GKE
from diagrams.gcp.ml import AutoML
...

# k8s resources

from diagrams.k8s.compute import Pod, StatefulSet
from diagrams.k8s.network import Service
from diagrams.k8s.storage import PV, PVC, StorageClass
...

# oracle resources

from diagrams.oci.compute import VirtualMachine, Container
from diagrams.oci.network import Firewall
from diagrams.oci.storage import FileStorage, StorageGateway
You can find lists of all available nodes for each provider in the sidebar on the left.

For example, here is the list of all available AWS nodes.

Data Flow
You can represent data flow by connecting the nodes with the operators >>, <<, and -.

> > connects nodes in left to right direction.
> > << connects nodes in right to left direction.

- connects nodes in no direction. Undirected.
  from diagrams import Diagram
  from diagrams.aws.compute import EC2
  from diagrams.aws.database import RDS
  from diagrams.aws.network import ELB
  from diagrams.aws.storage import S3

with Diagram("Web Services", show=False):
ELB("lb") >> EC2("web") >> RDS("userdb") >> S3("store")
ELB("lb") >> EC2("web") >> RDS("userdb") << EC2("stat")
(ELB("lb") >> EC2("web")) - EC2("web") >> RDS("userdb")
Be careful when using - and any shift operators together. It can cause unexpected results due to Python's operator precedence, so you might have to use parentheses.

web services diagram

The order of rendered diagrams is the reverse of the declaration order.

You can change the data flow direction with the direction parameter. The default is LR.

Allowed values are: TB, BT, LR, and RL

from diagrams import Diagram
from diagrams.aws.compute import EC2
from diagrams.aws.database import RDS
from diagrams.aws.network import ELB

with Diagram("Workers", show=False, direction="TB"):
lb = ELB("lb")
db = RDS("events")
lb >> EC2("worker1") >> db
lb >> EC2("worker2") >> db
lb >> EC2("worker3") >> db
lb >> EC2("worker4") >> db
lb >> EC2("worker5") >> db
workers diagram

Group Data Flow
The above worker example has too many redundant flows. To avoid this, you can group nodes into a list so that all nodes are connected to other nodes at once:

from diagrams import Diagram
from diagrams.aws.compute import EC2
from diagrams.aws.database import RDS
from diagrams.aws.network import ELB

with Diagram("Grouped Workers", show=False, direction="TB"):
ELB("lb") >> [EC2("worker1"),
EC2("worker2"),
EC2("worker3"),
EC2("worker4"),
EC2("worker5")] >> RDS("events")
grouped workers diagram

You can't connect two lists directly because shift/arithmetic operations between lists are not allowed in Python.

Clusters
Cluster allows you to group (or cluster) nodes in an isolated group.

Basic
Cluster represents a local cluster context.

You can create a cluster context using the Cluster class. You can also connect the nodes in a cluster to other nodes outside a cluster.

from diagrams import Cluster, Diagram
from diagrams.aws.compute import ECS
from diagrams.aws.database import RDS
from diagrams.aws.network import Route53

with Diagram("Simple Web Service with DB Cluster", show=False):
dns = Route53("dns")
web = ECS("service")

    with Cluster("DB Cluster"):
        db_primary = RDS("primary")
        db_primary - [RDS("replica1"),
                     RDS("replica2")]

    dns >> web >> db_primary

simple web service with db cluster diagram

Nested Clusters
Nested clustering is also possible:

from diagrams import Cluster, Diagram
from diagrams.aws.compute import ECS, EKS, Lambda
from diagrams.aws.database import Redshift
from diagrams.aws.integration import SQS
from diagrams.aws.storage import S3

with Diagram("Event Processing", show=False):
source = EKS("k8s source")

    with Cluster("Event Flows"):
        with Cluster("Event Workers"):
            workers = [ECS("worker1"),
                       ECS("worker2"),
                       ECS("worker3")]

        queue = SQS("event queue")

        with Cluster("Processing"):
            handlers = [Lambda("proc1"),
                        Lambda("proc2"),
                        Lambda("proc3")]

    store = S3("events store")
    dw = Redshift("analytics")

    source >> workers >> queue >> handlers
    handlers >> store
    handlers >> dw

event processing diagram

There is no depth limit to nesting. Feel free to create nested clusters as deep as you want.

Edges
Edge represents an edge between nodes.

Basic
Edge is an object representing a connection between nodes with some additional properties.

An edge object contains three attributes: label, color, and style. They mirror the corresponding Graphviz edge attributes.

from diagrams import Cluster, Diagram, Edge
from diagrams.onprem.analytics import Spark
from diagrams.onprem.compute import Server
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.aggregator import Fluentd
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.network import Nginx
from diagrams.onprem.queue import Kafka

with Diagram(name="Advanced Web Service with On-Premises (colored)", show=False):
ingress = Nginx("ingress")

    metrics = Prometheus("metric")
    metrics << Edge(color="firebrick", style="dashed") << Grafana("monitoring")

    with Cluster("Service Cluster"):
        grpcsvc = [
            Server("grpc1"),
            Server("grpc2"),
            Server("grpc3")]

    with Cluster("Sessions HA"):
        primary = Redis("session")
        primary \
            - Edge(color="brown", style="dashed") \
            - Redis("replica") \
            << Edge(label="collect") \
            << metrics
        grpcsvc >> Edge(color="brown") >> primary

    with Cluster("Database HA"):
        primary = PostgreSQL("users")
        primary \
            - Edge(color="brown", style="dotted") \
            - PostgreSQL("replica") \
            << Edge(label="collect") \
            << metrics
        grpcsvc >> Edge(color="black") >> primary

    aggregator = Fluentd("logging")
    aggregator \
        >> Edge(label="parse") \
        >> Kafka("stream") \
        >> Edge(color="black", style="bold") \
        >> Spark("analytics")

    ingress \
        >> Edge(color="darkgreen") \
        << grpcsvc \
        >> Edge(color="darkorange") \
        >> aggregator

advanced web service with on-premise diagram colored

Less Edges
As you can see on the previous graph the edges can quickly become noisy. Below are two examples to solve this problem.

One approach is to get creative with the Node class to create blank placeholders, together with named nodes within Clusters, and then only pointing to single named elements within those Clusters.

Compare the output below to the example output above .

from diagrams import Cluster, Diagram, Node
from diagrams.onprem.analytics import Spark
from diagrams.onprem.compute import Server
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.aggregator import Fluentd
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.network import Nginx
from diagrams.onprem.queue import Kafka

with Diagram("\nAdvanced Web Service with On-Premise Less edges", show=False) as diag:
ingress = Nginx("ingress")

    with Cluster("Service Cluster"):
        serv1 = Server("grpc1")
        serv2 = Server("grpc2")
        serv3 = Server("grpc3")

    with Cluster(""):
        blankHA = Node("", shape="plaintext", width="0", height="0")

        metrics = Prometheus("metric")
        metrics << Grafana("monitoring")

        aggregator = Fluentd("logging")
        blankHA >> aggregator >> Kafka("stream") >> Spark("analytics")

        with Cluster("Database HA"):
            db = PostgreSQL("users")
            db - PostgreSQL("replica") << metrics
            blankHA >> db

        with Cluster("Sessions HA"):
            sess = Redis("session")
            sess - Redis("replica") << metrics
            blankHA >> sess

    ingress >> serv2 >> blankHA

diag
advanced web service with on-premise less edges

Merged Edges
Yet another option is to set the graph_attr dictionary key "concentrate" to "true".

Note the following restrictions:

the Edge must end at the same headport
This only works when the "splines" graph_attr key is set to the value "spline". It has no effect when the value was set to "ortho", which is the default for the diagrams library.
this will only work with the "dot" layout engine, which is the default for the diagrams library.
For more information see:

https://graphviz.gitlab.io/doc/info/attrs.html#d:concentrate

https://www.graphviz.org/pdf/dotguide.pdf Section 3.3 Concentrators

from diagrams import Cluster, Diagram, Edge, Node
from diagrams.onprem.analytics import Spark
from diagrams.onprem.compute import Server
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.aggregator import Fluentd
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.network import Nginx
from diagrams.onprem.queue import Kafka

graph_attr = {
"concentrate": "true",
"splines": "spline",
}

edge_attr = {
"minlen":"3",
}

with Diagram("\n\nAdvanced Web Service with On-Premise Merged edges", show=False,
graph_attr=graph_attr,
edge_attr=edge_attr) as diag:

    ingress = Nginx("ingress")

    metrics = Prometheus("metric")
    metrics << Edge(minlen="0") << Grafana("monitoring")

    with Cluster("Service Cluster"):
        grpsrv = [
            Server("grpc1"),
            Server("grpc2"),
            Server("grpc3")]

    blank = Node("", shape="plaintext", height="0.0", width="0.0")

    with Cluster("Sessions HA"):
        sess = Redis("session")
        sess - Redis("replica") << metrics

    with Cluster("Database HA"):
        db = PostgreSQL("users")
        db - PostgreSQL("replica") << metrics

    aggregator = Fluentd("logging")
    aggregator >> Kafka("stream") >> Spark("analytics")

    ingress >> [grpsrv[0], grpsrv[1], grpsrv[2],]
    [grpsrv[0], grpsrv[1], grpsrv[2],] - Edge(headport="w", minlen="1") - blank
    blank >> Edge(headport="w", minlen="2") >> [sess, db, aggregator]

diag
advanced web service with on-premise merged edges
