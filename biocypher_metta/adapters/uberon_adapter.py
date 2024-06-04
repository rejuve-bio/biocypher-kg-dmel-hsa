from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class UberonAdapter(OntologyAdapter):
    ONTOLOGIES = {
            'uberon': 'http://purl.obolibrary.org/obo/uberon.owl'
        }

    def __init__(self, write_properties, add_provenance, ontology, type, label='uberon', dry_run=False):
        self.ontology = ontology
        
        super(UberonAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run)

    
    def get_ontology_source(self, ontology):
        """
        Returns the source and source URL for UBERON ontology.
        """
        if ontology == 'uberon':
            return 'UBERON', 'http://purl.obolibrary.org/obo/uberon.owl'
        else:
            return None, None
        