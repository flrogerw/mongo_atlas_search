import requests
import time
import os
from urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv
from typing import Optional, Dict, Any

# Load environment variables
load_dotenv()

# Disable SSL warnings
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


class OpenSearchModelManager:
    """
    A class to handle uploading, loading, and configuring a machine learning model in OpenSearch.
    """

    def __init__(self) -> None:
        """
        Initializes the OpenSearchModelManager by loading necessary environment variables.
        """
        self.host: Optional[str] = os.getenv('SEARCH_INSTANCE')
        self.vector_model_name: Optional[str] = os.getenv('VECTOR_MODEL_NAME')

        if not self.host or not self.vector_model_name:
            raise ValueError("Missing required environment variables: SEARCH_INSTANCE or VECTOR_MODEL_NAME")

        self.model_id: Optional[str] = None
        self.model_state: Optional[str] = None

    def upload_model(self) -> str:
        """
        Uploads a machine learning model to OpenSearch.

        Returns:
            str: The task ID associated with the upload request.

        Raises:
            Exception: If the model upload fails or no task ID is returned.
        """
        url = f"{self.host}/_plugins/_ml/models/_upload"
        payload = {
            "name": f"huggingface/sentence-transformers/{self.vector_model_name}",
            "version": "1.0.1",
            "model_format": "TORCH_SCRIPT"
        }

        try:
            response = requests.post(url, json=payload, verify=False)
            response.raise_for_status()
            response_data = response.json()
            task_id = response_data.get("task_id")

            if not task_id:
                raise ValueError("No task_id received after model upload.")

            print(f"Model upload initiated. Task ID: {task_id}")
            return task_id
        except requests.RequestException as e:
            raise Exception(f"Failed to upload model: {e}")

    def poll_for_model_id(self, task_id: str) -> str:
        """
        Polls OpenSearch until the model is registered and retrieves its model ID.

        Args:
            task_id (str): The task ID associated with model registration.

        Returns:
            str: The registered model ID.

        Raises:
            Exception: If the model ID is not obtained.
        """
        url = f"{self.host}/_plugins/_ml/tasks/{task_id}"
        while not self.model_id:
            print("Waiting for model registration to complete...")
            time.sleep(5)
            try:
                response = requests.get(url, verify=False).json()
                self.model_id = response.get("model_id")
            except requests.RequestException as e:
                raise Exception(f"Error while polling for model ID: {e}")

        print(f"Model registered successfully with ID: {self.model_id}")
        return self.model_id

    def load_model(self) -> str:
        """
        Loads the registered model into OpenSearch.

        Returns:
            str: The task ID for the model loading request.

        Raises:
            Exception: If model loading fails or no task ID is returned.
        """
        if not self.model_id:
            raise ValueError("Model ID is not set. Ensure model is uploaded and registered first.")

        url = f"{self.host}/_plugins/_ml/models/{self.model_id}/_load"

        try:
            response = requests.post(url, verify=False)
            response.raise_for_status()
            task_id = response.json().get("task_id")

            if not task_id:
                raise ValueError("No task_id received for model loading.")

            print(f"Model loading initiated. Task ID: {task_id}")
            return task_id
        except requests.RequestException as e:
            raise Exception(f"Failed to load model: {e}")

    def poll_for_model_state(self, task_id: str) -> str:
        """
        Polls OpenSearch until the model loading state is marked as 'COMPLETED'.

        Args:
            task_id (str): The task ID associated with the model loading process.

        Returns:
            str: The final model state.

        Raises:
            Exception: If model state remains unresolved or indicates an error.
        """
        url = f"{self.host}/_plugins/_ml/tasks/{task_id}"
        while not self.model_state:
            time.sleep(5)
            try:
                response = requests.get(url, verify=False).json()
                self.model_state = response.get("state")
                print(f"Waiting for model state to be COMPLETED. Current state: {self.model_state}")

                if self.model_state == "COMPLETED":
                    break
            except requests.RequestException as e:
                raise Exception(f"Error while polling model state: {e}")

        print(f"Model successfully loaded and is in state: {self.model_state}")
        return self.model_state

    def create_neural_search_pipeline(self) -> None:
        """
        Creates an ingestion pipeline in OpenSearch for neural search.

        Raises:
            Exception: If the pipeline creation fails.
        """
        if not self.model_id:
            raise ValueError("Model ID is not set. Ensure model is uploaded and registered first.")

        url = f"{self.host}/_ingest/pipeline/neural_pipeline"
        payload: Dict[str, Any] = {
            "description": "Neural search pipeline for semantic search",
            "processors": [
                {
                    "text_embedding": {
                        "model_id": self.model_id,
                        "field_map": {
                            "description": "description_vector"
                        }
                    }
                }
            ]
        }

        try:
            response = requests.put(url, json=payload, verify=False)
            response.raise_for_status()
            print("Neural search pipeline created successfully.")
            print(f"Model ID: {self.model_id}")
        except requests.RequestException as e:
            raise Exception(f"Failed to create ingestion pipeline: {e}")


if __name__ == "__main__":
    """
    Executes the model upload, registration, loading, and pipeline creation steps sequentially.
    """
    try:
        model_manager = OpenSearchModelManager()
        upload_task_id = model_manager.upload_model()
        model_id = model_manager.poll_for_model_id(upload_task_id)
        load_task_id = model_manager.load_model()
        model_state = model_manager.poll_for_model_state(load_task_id)
        model_manager.create_neural_search_pipeline()
    except Exception as error:
        print(f"Error: {error}")
