import pathlib
import os
from biocypher import BioCypher
from biocypher._logger import logger
import networkx as nx


class Neo4jWriter:

    def __init__(self, schema_config, biocypher_config, output_dir):
        self.schema_config = schema_config
        self.biocypher_config = biocypher_config
        self.output_path = pathlib.Path(output_dir)

        if not os.path.exists(output_dir):
            self.output_path.mkdir()

        self.bcy = BioCypher(
            schema_config_path=schema_config, biocypher_config_path=biocypher_config
        )

        self.onotology = self.bcy._get_ontology()
        self.create_edge_types()

        self.excluded_properties = []

    def create_edge_types(self):
            schema = self.bcy._get_ontology_mapping()._extend_schema()
            self.edge_node_types = {}

            for k, v in schema.items():
                if (
                    v["represented_as"] == "edge"
                ):  # (: (label $x $y) (-> source_type target_type
                    edge_type = self.convert_input_labels(k)
                    source_type = v.get("source", None)
                    target_type = v.get("target", None)

                    if source_type is not None and target_type is not None:
                        # ## TODO fix this in the scheme config
                        if isinstance(v["input_label"], list):
                            label = self.convert_input_labels(v["input_label"][0])
                            source_type = self.convert_input_labels(source_type[0])
                            target_type = self.convert_input_labels(target_type[0])
                        else:
                            label = self.convert_input_labels(v["input_label"])
                            source_type = self.convert_input_labels(source_type)
                            target_type = self.convert_input_labels(target_type)
                        output_label = v.get("output_label", None)

                        self.edge_node_types[label.lower()] = {
                            "source": source_type.lower(),
                            "target": target_type.lower(),
                            "output_label": (
                                output_label.lower() if output_label is not None else None
                            ),
                        }
    def write_nodes(self, nodes, path_prefix=None, create_dir=True):
        if path_prefix is not None:
            file_path = f"{self.output_path}/{path_prefix}/nodes.cypher"
            if create_dir:
                if not os.path.exists(f"{self.output_path}/{path_prefix}"):
                    pathlib.Path(f"{self.output_path}/{path_prefix}").mkdir(
                        parents=True, exist_ok=True
                    )
        else:
            file_path = f"{self.output_path}/nodes.cypher"

        with open(file_path, "a") as f:
            for node in nodes:
                query = self.write_node(node)
                f.write(query + "\n")

        logger.info("Finished writing out nodes")
    
    def write_node(self, node):
        id, label, properties = node
        if "." in label:
            label = label.split(".")[1]
        label = label.lower()
        id = id.lower()
        properties_str = self._format_properties(properties)
        return f"CREATE (:{label} {{id: '{id}',{properties_str}}})"
    
    def _format_properties(self, properties):
        """
        Format properties into a Cypher string.
        :param properties: Dictionary of properties
        :return: Cypher formatted properties string
        """
        out_str = []
        for k, v in properties.items():
            if k in self.excluded_properties or v is None or v == "":
                continue
            if isinstance(v, list):
                prop = "[" + ", ".join(f"'{e}'" for e in v) + "]"
            elif isinstance(v, dict):
                prop = self._format_properties(v)
            else:
                prop = f"'{v}'"
            out_str.append(f"{k}: {prop}")
        return ", ".join(out_str)

    def convert_input_labels(self, label, replace_char="_"):
        """
        A method that removes spaces in input labels and replaces them with replace_char
        :param label: Input label of a node or edge
        :param replace_char: the character to replace spaces with
        :return:
        """
        return label.replace(" ", replace_char)

    def get_parent(self, G, node):
        """
        Get the immediate parent of a node in the ontology.
        """
        return nx.dfs_preorder_nodes(G, node, depth_limit=2)


