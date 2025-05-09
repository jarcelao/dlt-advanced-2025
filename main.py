import dlt
import os

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


os.environ["EXTRACT__WORKERS"] = "4"
os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "10000"
os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = "10000"

os.environ["NORMALIZE__WORKERS"] = "4"
os.environ["NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS"] = "10000"
os.environ["NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS"] = "10000"

os.environ["LOAD__WORKERS"] = "4"


client = RESTClient(
    base_url="https://jaffle-shop.scalevector.ai/api/v1",
    paginator=PageNumberPaginator(base_page=1, total_path=""),
)


@dlt.resource(
    table_name="customers",
    write_disposition="merge",
    primary_key="id",
    parallelized=True,
)
def get_customers():
    yield from client.paginate("/customers")


@dlt.resource(
    table_name="orders", write_disposition="merge", primary_key="id", parallelized=True
)
def get_orders():
    yield from client.paginate("/orders?page_size=1000")


@dlt.resource(
    table_name="products",
    write_disposition="merge",
    primary_key="sku",
    parallelized=True,
)
def get_products():
    yield from client.paginate("/products")


@dlt.source
def jaffle_shop_data():
    return get_customers(), get_orders(), get_products()


pipeline = dlt.pipeline(
    pipeline_name="jaffle_shop_pipeline",
    destination="duckdb",
    dataset_name="jaffle_shop",
    dev_mode=True,
    progress=dlt.progress.log(),
)


def main():
    pipeline.run(jaffle_shop_data())
    print(pipeline.last_trace)


if __name__ == "__main__":
    main()
