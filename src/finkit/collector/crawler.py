import csv
import logging


def save(file: str, header: [], data: []):
    # save as csv file
    with open(file, "w", newline="") as f:
        writer = csv.DictWriter(f, header)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    logging.info("stored csv file: %s", file)


class Crawler:

    def crawl(self):
        # 抓取数据，存入data对象
        pass
