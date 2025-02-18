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

MILEI_HOST = "10.247.171.177:7700"
OPEN_HOST = "10.247.171.134"
SOLR_HOST = "10.247.171.53:8983"


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
                #print(f"Error on doc: {doc}")
                pass
        elapsed = time.time() - start
        return elapsed

    def search(self, query, num_requests=100, concurrency=10):
        payload = {"query": {"multi_match": {"query": query}}}
        return self._run_search_test(payload, num_requests, concurrency, use_get=False)

    def _perform_search(self, payload):
        start = time.time()
        try:
            self.client.search(body=payload, index='misp-galaxies')
            elapsed = time.time() - start
            return elapsed
        except Exception as e:
            return None, e

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
        
        #print(f"latencies:{latencies}")
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

        #print(latencies)
        return {
            "total_requests": total_requests,
            "successful": len(latencies),
            "errors": errors,
            "avg_latency": avg_latency,
            "median_latency": median_latency,
            "throughput": throughput,
        }

    def cleanup(self):
        self.client.delete_index('misp-galaxies')

# Solr implementation
class SolrEngine(SearchEngine):
    def __init__(self):
        super().__init__("Solr")
        self.solr = pysolr.Solr(f'http://{SOLR_HOST}/solr/', always_commit=True)

    def index_documents(self, docs):
        self.solr.ping()
        start = time.time()
        self.solr.add(docs)
        elapsed = time.time() - start
        return elapsed

    def search(self, query, num_requests=100, concurrency=10):
        # Solr uses GET with query parameters.
        params = {"q": query, "wt": "json"}
        return self._run_search_test(params, num_requests, concurrency)

    def _perform_get_search(self, params):
        start = time.time()
        try:
            response = requests.get(
                self.search_url,
                params=params,
                headers={"Content-Type": "application/json"},
            )
            elapsed = time.time() - start
            return elapsed, response.status_code
        except Exception as e:
            return None, e

    def _run_search_test(self, params, num_requests, concurrency):
        latencies = []
        errors = 0
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for _ in range(num_requests):
                futures.append(executor.submit(self._perform_get_search, params))
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    latency, status = result
                    if (
                        latency is not None
                        and isinstance(status, int)
                        and status == 200
                    ):
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


# Tester class that loads the dataset and runs all tests
class MISPPerfTester:
    def __init__(self, dataset_dir, num_index, num_search, concurrency, query):
        self.dataset_dir = dataset_dir
        self.num_index = num_index
        self.num_search = num_search
        self.concurrency = concurrency
        self.query = query
        self.documents = self.load_dataset()
        # Create instances for each search engine
        self.engines = [OpenSearchEngine(), MeilisearchEngine()] #, SolrEngine()]

    def load_dataset(self):
        print(f"Loading dataset from directory: {self.dataset_dir}")
        docs = []
        for filename in os.listdir(self.dataset_dir):
            if filename.endswith(".json"):
                file_path = os.path.join(self.dataset_dir, filename)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        doc = json.load(f)
                        for cluster in doc["values"]:
                            docs.append(cluster)
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
    # docs = tester.documents
    # meili = tester.engines[1]
    # print(meili.index_documents(docs))
    # print(meili._run_search_test(tester.query, tester.num_search, tester.concurrency))

    # opensearch = tester.engines[0]
    # opensearch.index_documents(docs)
    # print(opensearch._run_search_test({"query": {"multi_match": {"query": tester.query}}}, tester.num_search, tester.concurrency))
    
    # solr = tester.engines[2]
    # print(solr.index_documents(docs))

    # tester.cleanup()
    # tester.run_indexing_tests()

    tester.run_search_tests()
if __name__ == "__main__":
    main()
