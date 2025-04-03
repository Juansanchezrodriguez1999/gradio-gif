
# Image Statistics Application

This repository hosts an application for generating statistics from uploaded images, presenting the results in graphical and tabular formats. The application leverages [Gradio](https://gradio.app/) to provide an intuitive interface for processing image data.

---

## Features

- **Image Upload**: Users can upload multiple images for analysis.
- **Statistics Generation**: Computes various statistics from image data, including time-series analysis.
- **Graphical Outputs**: Produces visual plots such as monthly trends and totals.
- **CSV Export**: Downloads a CSV file with processed data.
- **Data Preview**: Displays the resulting data as a table for quick reference.
- **Authentication**: Secured login functionality using environment variables.

---

## Prerequisites

Before running the application, ensure you have the following:

- Python 3.8+
- Required Python dependencies (see below)
- A `.env` file with the following content:
  ```
  DUMMY_USER=<your_username>
  DUMMY_PW=<your_password>
  API_KEY=<your_api_key>
  ```

---


## Usage

### Running Locally

Start the application by running:
```bash
uvicorn app:app --reload
```
Visit `http://127.0.0.1:8000` to access the Gradio interface.

### Interface

- **Inputs**:
  - Upload multiple image files (supported formats depend on the application setup).

- **Outputs**:
  - **Download Graphs**: A ZIP file containing statistical plots.
  - **Download CSV**: A CSV file summarizing the image statistics.
  - **Temporal Series Table**: A preview of the calculated data in tabular form.

---


## Development

### Code Structure

- Modular functions for statistics calculation and plot generation.
- User interface created with Gradio for ease of use.
- Backend API built with FastAPI for authentication and data handling.

### Dependencies

Install required libraries:
```bash
pip install -r requirements.txt
```

Key libraries used:
- `gradio`: For the user interface.
- `fastapi`: For backend API support.
- `pandas`: For data manipulation and analysis.
- `os`, `zipfile`: For file operations.

---

## License

This project is licensed under the MIT License. See `LICENSE` for details.

---

## Acknowledgements

- [Gradio](https://gradio.app/) for simplifying UI creation.
- [Pandas](https://pandas.pydata.org/) for efficient data processing.
- The Python community for providing exceptional tools and libraries.