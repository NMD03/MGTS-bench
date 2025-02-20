import argparse
import os
import json
import requests
import time
import concurrent.futures
import meilisearch
import pysolr
from statistics import mean, median
from opensearchpy import OpenSearch
import matplotlib.pyplot as plt
import numpy as np

MILEI_HOST = "10.201.235.43:7700"
OPEN_HOST = "10.201.235.220"
SOLR_HOST = "10.201.235.34:8983"
SOLR_CORE = "new_core"


# Base class for search engines
class SearchEngine:
    def __init__(self, name):
        self.name = name

    def index_documents(self, docs):
        """Index a list of documents. Must be implemented in subclasses."""
        raise NotImplementedError

    def search(self, query, num_requests=100, concurrency=10):
        """Run search queries and return performance metrics. Must be implemented in subclasses."""
        raise NotImplementedError


# OpenSearch implementation
class OpenSearchEngine(SearchEngine):
    def __init__(self):
        super().__init__("OpenSearch")
        self.ca_certs_path = "./root-ca.pem"
        self.client = OpenSearch(
            hosts=[{"host": OPEN_HOST, "port": 9200}],
            http_compress=True,  # enables gzip compression for request bodies
            http_auth=("admin", "RandomShit1!"),
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            ca_certs=self.ca_certs_path,
        )
        self.index_name = "misp-galaxies"
        self.index_body = {"settings": {"index": {"number_of_shards": 4}}}

    def index_documents(self, docs):
        start = time.time()
        self.client.indices.create(self.index_name, body=self.index_body)
        for doc in docs:
            try:
                doc_id = doc.get("id", str(hash(json.dumps(doc))))
                self.client.index(
                    index="misp-galaxies", body=doc, id=doc_id, refresh=True
                )
            except Exception:
                # print(f"Error on doc: {doc}")
                pass
        elapsed = time.time() - start
        return elapsed

    def search(self, query, num_requests=100, concurrency=10):
        payload = {"query": {"multi_match": {"query": query}}}
        return self._run_search_test(payload, num_requests, concurrency, use_get=False)

    def _perform_search(self, payload):
        start = time.time()
        try:
            self.client.search(body=payload, index="misp-galaxies")
            elapsed = time.time() - start
            return elapsed
        except Exception as e:
            return None

    def _run_search_test(self, payload, num_requests, concurrency, use_get=False):
        latencies = []
        errors = 0
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for _ in range(num_requests):
                futures.append(executor.submit(self._perform_search, payload))
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    latency = result
                    if latency is not None:
                        latencies.append(latency)
                    else:
                        errors += 1
                else:
                    errors += 1

        # print(f"latencies:{latencies}")
        total_time = time.time() - start_time
        total_requests = len(latencies) + errors
        avg_latency = mean(latencies) if latencies else 0
        median_latency = median(latencies) if latencies else 0
        throughput = total_requests / total_time if total_time > 0 else 0

        return {
            "total_requests": total_requests,
            "successful": len(latencies),
            "errors": errors,
            "avg_latency": avg_latency,
            "median_latency": median_latency,
            "throughput": throughput,
        }

    def cleanup(self):
        self.client.indices.delete(index="misp-galaxies")


# Meilisearch implementation
class MeilisearchEngine(SearchEngine):
    def __init__(self):
        super().__init__("Meilisearch")
        self.client = meilisearch.Client(f"http://{MILEI_HOST}")

    def index_documents(self, docs):
        start = time.time()
        self.client.create_index("misp-galaxies", {"primaryKey": "uuid"})
        for doc in docs:
            self.client.index("misp-galaxies").update_documents([doc])
        elapsed = time.time() - start
        return elapsed

    def search(self, query, num_requests=100, concurrency=10):
        payload = query
        return self._run_search_test(payload, num_requests, concurrency)

    def _perform_search(self, payload):
        start = time.time()
        self.client.index("misp-galaxies").search(payload)
        elapsed = time.time() - start
        return elapsed

    def _run_search_test(self, payload, num_requests, concurrency):
        latencies = []
        errors = 0
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for _ in range(num_requests):
                futures.append(executor.submit(self._perform_search, payload))
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    latency = result
                    if latency is not None:
                        latencies.append(latency)
                    else:
                        errors += 1
                else:
                    errors += 1

        total_time = time.time() - start_time
        total_requests = len(latencies) + errors
        avg_latency = mean(latencies) if latencies else 0
        median_latency = median(latencies) if latencies else 0
        throughput = total_requests / total_time if total_time > 0 else 0

        # print(latencies)
        return {
            "total_requests": total_requests,
            "successful": len(latencies),
            "errors": errors,
            "avg_latency": avg_latency,
            "median_latency": median_latency,
            "throughput": throughput,
        }

    def cleanup(self):
        self.client.delete_index("misp-galaxies")


# Solr implementation
class SolrEngine(SearchEngine):
    def __init__(self):
        super().__init__("Solr")
        self.solr = pysolr.Solr(
            f"http://{SOLR_HOST}/solr/{SOLR_CORE}", always_commit=True
        )
        self.schema_url = f"http://{SOLR_HOST}/solr/{SOLR_CORE}/schema"
        self.created_fields = {}
        self.global_field_defs = {}

    def add_field(
        self, field_name, field_type, stored=True, indexed=True, multiValued=False
    ):
        """
        Adds a single field to the schema using the Solr Schema API.
        Updates the global state so the field is not re-added.
        """
        # Dirty fix for revoked
        if field_name == "revoked":
            field_type = "booleans"

        payload = {
            "add-field": {
                "name": field_name,
                "type": "text_en",
                "stored": stored,
                "indexed": indexed,
                "multiValued": multiValued,
            }
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(self.schema_url, json=payload, headers=headers)
        if response.status_code != 200:
            print(f"Error adding field '{field_name}': {response.text}")
        else:
            print(
                f"Successfully added field '{field_name}' with type '{field_type}' and multiValued={multiValued}."
            )
            self.created_fields[field_name] = {
                "type": field_type,
                "multiValued": multiValued,
            }
        return response.json()

    def field_exists(self, field_name):
        """
        Checks if a field exists in the Solr schema.
        """
        url = f"{self.schema_url}/fields/{field_name}"
        response = requests.get(url)
        return response.status_code == 200

    def infer_field_definition(self, value):
        """
        Infers a basic Solr field type and multiValued property based on the value.
        If the value is a list, it returns a multiValued definition.
        """
        if isinstance(value, list):
            if not value:
                return {"type": "strings", "multiValued": True}
            first_elem = value[0]
            if isinstance(first_elem, int):
                return {"type": "pints", "multiValued": True}
            elif isinstance(first_elem, float):
                return {"type": "pfloats", "multiValued": True}
            elif isinstance(first_elem, bool):
                return {"type": "string", "multiValued": True}
            elif isinstance(first_elem, dict):
                # For nested dicts, we assume flattening will be done.
                return {"type": "text_general", "multiValued": False}
            else:
                return {"type": "strings", "multiValued": True}
        else:
            if isinstance(value, int):
                return {"type": "pint", "multiValued": False}
            elif isinstance(value, float):
                return {"type": "pfloat", "multiValued": False}
            elif isinstance(value, bool):
                return {"type": "boolean", "multiValued": False}
            elif isinstance(value, dict):
                # We will flatten nested dicts so this branch is not used for field creation.
                return {"type": "text_general", "multiValued": False}
            else:
                return {"type": "string", "multiValued": False}

    def _gather_field_definitions_from_doc(self, doc, field_defs, flatten=True):
        """
        Recursively collects field definitions from the document (or nested sub-documents).
        If the same field appears more than once with conflicting types or multiValued flags,
        we merge them: if any occurrence is multiValued, the final definition is multiValued;
        if types differ, we default to string(s).
        """
        for key, value in doc.items():
            if flatten and isinstance(value, dict):
                self._gather_field_definitions_from_doc(value, field_defs, flatten)
            elif (
                flatten
                and isinstance(value, list)
                and value
                and isinstance(value[0], dict)
            ):
                for item in value:
                    self._gather_field_definitions_from_doc(item, field_defs, flatten)
            else:
                inferred = self.infer_field_definition(value)
                if key in field_defs:
                    existing = field_defs[key]
                    # Merge multiValued: if any occurrence is multiValued, set multiValued True.
                    merged_multi = existing["multiValued"] or inferred["multiValued"]
                    # If types differ, default to string (or strings if multiValued).
                    merged_type = existing["type"]
                    if existing["type"] != inferred["type"]:
                        merged_type = "strings" if merged_multi else "string"
                    field_defs[key] = {"type": merged_type, "multiValued": merged_multi}
                else:
                    field_defs[key] = inferred

    def gather_field_definitions(self, docs, flatten=True):
        """
        Scans all documents and returns a dictionary of field definitions.
        """
        field_defs = {}
        for doc in docs:
            self._gather_field_definitions_from_doc(doc, field_defs, flatten)
        return field_defs

    def create_core_schema(self, docs):
        """
        Pre-scans the documents to determine the unified field definitions and
        creates the fields in the Solr schema once.
        """
        field_defs = self.gather_field_definitions(docs, flatten=True)
        print("Unified field definitions gathered:")
        for field, definition in field_defs.items():
            print(f"  {field}: {definition}")
            # Only attempt to add if not already in global state and not existing in schema.
            if field in self.created_fields:
                print(f"Field '{field}' already added (global state); skipping.")
                continue
            if self.field_exists(field):
                print(
                    f"Field '{field}' already exists in schema; adding to global state."
                )
                self.created_fields[field] = definition
                continue
            self.add_field(
                field_name=field,
                field_type=definition["type"],
                stored=True,
                indexed=True,
                multiValued=definition.get("multiValued", False),
            )

        self.add_catchall_field()

    def add_catchall_field(self):
        """
        Ensures a catch-all field '_text_' exists and adds a copyField rule
        to copy all fields into it so that searches run against _text_ aggregate.
        """
        # Create the catch-all field if it doesn't exist.
        if not self.field_exists("_text_"):
            print("Creating catch-all field '_text_'.")
            self.add_field(
                "_text_", "text_general", stored=False, indexed=True, multiValued=True
            )
        else:
            print("Catch-all field '_text_' already exists.")
        # Add the copyField rule to aggregate all fields into _text_
        payload = {"add-copy-field": {"source": "*", "dest": "_text_"}}
        headers = {"Content-Type": "application/json"}
        response = requests.post(self.schema_url, json=payload, headers=headers)
        if response.status_code != 200:
            print(f"Error adding copyField rule: {response.text}")
        else:
            print("Successfully added copyField rule from '*' to '_text_'.")

    def index_documents(self, docs):
        self.solr.ping()
        start = time.time()
        self.solr.add(docs)
        elapsed = time.time() - start
        return elapsed

    def search(self, query, num_requests=100, concurrency=10):
        params = query
        return self._run_search_test(params, num_requests, concurrency)

    def _perform_search(self, params):
        start = time.time()
        results = self.solr.search(params, **{"rows": 1000})
        elapsed = time.time() - start
        # print("Saw {0} result(s).".format(len(results)))
        # for result in results:
        #     print("The title is '{0}'.".format(result))
        return elapsed

    def _run_search_test(self, params, num_requests, concurrency):
        latencies = []
        errors = 0
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for _ in range(num_requests):
                futures.append(executor.submit(self._perform_search, params))
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    latency = result
                    if latency is not None:
                        latencies.append(latency)
                    else:
                        errors += 1
                else:
                    errors += 1

        total_time = time.time() - start_time
        total_requests = len(latencies) + errors
        avg_latency = mean(latencies) if latencies else 0
        median_latency = median(latencies) if latencies else 0
        throughput = total_requests / total_time if total_time > 0 else 0

        return {
            "total_requests": total_requests,
            "successful": len(latencies),
            "errors": errors,
            "avg_latency": avg_latency,
            "median_latency": median_latency,
            "throughput": throughput,
        }

    def cleanup(self):
        self.solr.delete(q="*:*")


# Tester class that loads the dataset and runs all tests
class MISPPerfTester:
    def __init__(self, dataset_dir, num_index, num_search, concurrency, query):
        self.dataset_dir = dataset_dir
        self.num_index = num_index
        self.num_search = num_search
        self.concurrency = concurrency
        self.query = query
        self.nested = True
        self.documents = self.load_dataset()
        # Create instances for each search engine
        self.engines = [OpenSearchEngine(), MeilisearchEngine(), SolrEngine()]

    def load_dataset(self):
        print(f"Loading dataset from directory: {self.dataset_dir}")
        docs = []
        for filename in os.listdir(self.dataset_dir):
            if filename.endswith(".json"):
                file_path = os.path.join(self.dataset_dir, filename)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        doc = json.load(f)
                        if self.nested:
                            for cluster in doc["values"]:
                                docs.append(cluster)
                        else:
                            docs.append(doc)
                except Exception as e:
                    print(f"Error loading {file_path}: {e}")
        print(f"Loaded {len(docs)} documents from {self.dataset_dir}")
        # Use only the first num_index documents
        return docs[: self.num_index]

    def run_indexing_tests(self):
        print("=" * 50)
        print("INDEXING PERFORMANCE TESTS")
        print("=" * 50)
        for engine in self.engines:
            print(f"\nIndexing documents into {engine.name}...")
            elapsed = engine.index_documents(self.documents)
            throughput = len(self.documents) / elapsed if elapsed > 0 else 0
            print(f"{engine.name} Indexing Results:")
            print(f"  Time Taken        : {elapsed:.2f} seconds")
            print(f"  Throughput        : {throughput:.2f} docs/sec")
            print("-" * 50)

    def run_search_tests(self):
        print("\nWaiting for indexes to refresh...")
        time.sleep(3)
        print("\n" + "=" * 50)
        print("SEARCH PERFORMANCE TESTS")
        print("=" * 50)
        for engine in self.engines:
            print(
                f"\nRunning search tests for {engine.name} with query '{self.query}'..."
            )
            result = engine.search(self.query, self.num_search, self.concurrency)
            print(f"{engine.name} Search Performance:")
            print(f"  Total Requests    : {result['total_requests']}")
            print(f"  Successful        : {result['successful']}")
            print(f"  Errors            : {result['errors']}")
            print(f"  Avg Latency       : {result['avg_latency'] * 1000:.2f} ms")
            print(f"  Median Latency    : {result['median_latency'] * 1000:.2f} ms")
            print(f"  Throughput        : {result['throughput']:.2f} req/s")
            print("-" * 50)

    def run_multiple_search_tests(self):
        # Define a list of queries to test.
        queries = ["APT28", "Android", "phishing", "malware", "fraud", "e878d24d-f122-48c4-930c-f6b6d5f0ee28"]
        metrics = ["avg_latency", "median_latency", "throughput", "errors"]
        # Prepare a dictionary to store metrics per engine per query.
        results = {engine.name: {m: [] for m in metrics} for engine in self.engines}
        print("\nRunning multi-query search tests...")
        for q in queries:
            print(f"Query: '{q}'")
            for engine in self.engines:
                res = engine.search(q, self.num_search, self.concurrency)
                results[engine.name]["avg_latency"].append(
                    res["avg_latency"] * 1000
                )  # convert seconds to ms
                results[engine.name]["median_latency"].append(
                    res["median_latency"] * 1000
                )
                results[engine.name]["throughput"].append(res["throughput"])
                results[engine.name]["errors"].append(res["errors"])
                print(
                    f"  {engine.name}: avg {res['avg_latency'] * 1000:.2f} ms, median {res['median_latency'] * 1000:.2f} ms, throughput {res['throughput']:.2f}, errors {res['errors']}"
                )
        return queries, results

    def plot_results(self, queries, results):
        # We'll create a 2x2 grid: one subplot for each metric.
        metrics = ["avg_latency", "median_latency", "throughput", "errors"]
        titles = {
            "avg_latency": "Average Latency (ms)",
            "median_latency": "Median Latency (ms)",
            "throughput": "Throughput (req/s)",
            "errors": "Errors (#)",
        }
        engine_names = list(results.keys())
        num_queries = len(queries)
        x = np.arange(num_queries)
        width = 0.2  # width of each bar
        fig, axs = plt.subplots(2, 2, figsize=(12, 10))
        axs = axs.flatten()
        for idx, metric in enumerate(metrics):
            ax = axs[idx]
            for i, engine in enumerate(engine_names):
                ax.bar(x + i * width, results[engine][metric], width, label=engine)
            ax.set_xticks(x + width * (len(engine_names) - 1) / 2)
            ax.set_xticklabels(queries)
            ax.set_xlabel("Query")
            ax.set_ylabel(titles[metric])
            ax.set_title(titles[metric])
            ax.legend()
            plt.tight_layout()
            plt.savefig("search_performance.png")
            print("Plot saved to search_performance.png")

    def cleanup(self):
        for engine in self.engines:
            engine.cleanup()


def main():
    parser = argparse.ArgumentParser(
        description="Performance tests for indexing and search using the MISP Galaxies dataset."
    )
    parser.add_argument(
        "--dataset-dir",
        required=True,
        help="Path to the directory containing JSON files",
    )
    parser.add_argument(
        "--query", default="APT28", help="Search query (default: APT28)"
    )
    parser.add_argument(
        "--num-index",
        type=int,
        default=50000,
        help="Max number of documents to index (default: 50000)",
    )
    parser.add_argument(
        "--num-search",
        type=int,
        default=100,
        help="Number of search requests (default: 100)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Concurrency level for search requests (default: 10)",
    )
    args = parser.parse_args()

    tester = MISPPerfTester(
        args.dataset_dir, args.num_index, args.num_search, args.concurrency, args.query
    )
    # solr = tester.engines[2]
    # solr.create_core_schema(tester.documents)
    #
    # tester.cleanup()
    # tester.run_indexing_tests()
    # tester.run_search_tests()
    queries, results = tester.run_multiple_search_tests()
    tester.plot_results(queries, results)


if __name__ == "__main__":
    main()
