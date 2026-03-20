import os

from huggingface_hub import snapshot_download


def main() -> None:
    repos = [
        os.environ["WHISPERX_MODEL_REPO"],
        os.environ["WHISPERX_ALIGN_MODEL_REPO"],
    ]
    cache_dir = os.environ["HF_HOME"]

    for repo_id in repos:
        print(f"Preloading Hugging Face model cache: {repo_id}", flush=True)
        snapshot_download(
            repo_id=repo_id,
            cache_dir=cache_dir,
        )


if __name__ == "__main__":
    main()
