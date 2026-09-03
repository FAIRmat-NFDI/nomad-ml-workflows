from nomad.config.models.plugins import SchemaPackageEntryPoint


class DatasetSchemaEntryPoint(SchemaPackageEntryPoint):
    def load(self):
        from nomad_ml_workflows.schema_packages.dataset import m_package

        return m_package


class ModelSchemaEntryPoint(SchemaPackageEntryPoint):
    def load(self):
        from nomad_ml_workflows.schema_packages.model import m_package

        return m_package


dataset_schema = DatasetSchemaEntryPoint(
    name='Dataset Schema',
    description='Schema package containing schema definitions for ML datasets.',
)  # type: ignore
model_schema = ModelSchemaEntryPoint(
    name='Model Schema',
    description='Schema package containing schema definitions for ML models.',
)  # type: ignore
