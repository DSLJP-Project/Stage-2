## How to Run the Project

1. Open the terminal in the project's root directory and run the **Maven build** commands:
   ```
   mvn clean package
   ```

2. Then, **stop and remove** any previous Docker instances:
  ```
  docker compose down
  ```

3. Next, **build and start** the containers:
  ```
  docker compose up --build
  ```
This will start the Docker containers and store the books.

4. To perform searches, go to the **`search-service`** module and run the **`LocalCli`** class.

---
