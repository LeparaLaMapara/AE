
from analytics_engine import BaseAnalyticsEngine
from pyspark.sql.functions import avg, sum as _sum

class SalesAggregator(BaseAnalyticsEngine):
    def transforms(self, inputs):
        df = inputs['sales_data']
        # Perform aggregation: average price and total quantity sold per product
        aggregated_df = df.groupBy('product_id').agg(
            avg('price').alias('average_price'),
            _sum('quantity_sold').alias('total_quantity_sold')
        )
        
        # Log some example parameters and metrics
        self.log_param("aggregation_type", "average")
        self.log_metric("rows_processed", df.count())
        
        # If you train a model, you can log it here
        # self.log_model(model, "sales_model")
        
        return aggregated_df
