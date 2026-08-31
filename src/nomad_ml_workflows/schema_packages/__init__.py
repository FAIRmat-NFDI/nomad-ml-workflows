from nomad.config.models.plugins import SchemaPackageEntryPoint


class ModelSchemaEntryPoint(SchemaPackageEntryPoint):
    def load(self):
        from nomad_ml_workflows.schema_packages.model import m_package

        return m_package


model_schema = ModelSchemaEntryPoint(
    name='Model Schema',
    description='Schema package containing schema definitions for ML models.',
)
