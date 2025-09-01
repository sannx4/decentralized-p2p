from util.metrics import snapshot
import json

if __name__ == "__main__":
    print(json.dumps(snapshot(), indent=2, sort_keys=True))
