import importlib.util

def load_job(path):
    spec = importlib.util.spec_from_file_location("etl_job", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
