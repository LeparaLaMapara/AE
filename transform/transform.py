from analytics_engine import BaseAnalyticsEngine

class DataTransformer(BaseAnalyticsEngine):

    def extract(self):
        # Load inputs as defined in the config.yaml
        inputs = self.load_inputs_from_config()
        return inputs

    def transforms(self, inputs):
        # Perform data transformation on the inputs
        # Example transformation: Joining multiple inputs
        df1 = inputs['source1']
        df2 = inputs['source2']
        transformed_data = df1.join(df2, "common_column")
        return transformed_data
