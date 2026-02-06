import uvicorn


def main():
    print("Starting server at http://localhost:8000")
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
