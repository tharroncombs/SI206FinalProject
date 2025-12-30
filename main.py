import logging

from data_visualization import visualize_all_data

if __name__ == "__main__":
    try:
        visualize_all_data()
    except Exception as e:
        logging.error("Visualization script failed: %s", e)
        raise
