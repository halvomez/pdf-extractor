import os
import logging
import argparse
import fitz
from pathlib import Path
from multiprocessing import Pool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('log')
logger.setLevel('INFO')

parser = argparse.ArgumentParser()
parser.add_argument('input_dir', help='specify input dir path for convert')
parser.add_argument('-out', '--output_dir',
                    help='specify output dir path for save jpg')
parser.add_argument('-q', '--quality', type=float, default=2.5,
                    help='specify convert quality')
args = parser.parse_args()


def collect_pdf_for_convert(input_dir):
    _pdfs = []
    for (root, dirs, files) in os.walk(input_dir, topdown=True):
        for file in files:
            if file.split('.')[-1].lower() == 'pdf':
                _pdfs.append(os.path.join(root, file))

    return _pdfs


def saving_separate_pages(file):
    logger.info(f' converting {file}')
    processing_path = str(Path(file).parent)

    fpath_parts = Path(file).parts
    ipath_parts = Path(args.input_dir).parts

    if not args.output_dir:
        target_path = processing_path
    else:
        target_path = os.path.join(args.output_dir,
                                   *fpath_parts[len(ipath_parts):-1])

    if os.path.exists(processing_path):
        os.makedirs(processing_path, exist_ok=True)

    fname = str(Path(file).stem)

    if not os.path.exists(target_path):
        os.makedirs(target_path, exist_ok=True)

    with open(file, 'rb') as file_:
        with fitz.Document(fname, file_.read()) as doc:
            if doc.pageCount > 1:
                for page in doc:
                    mat = fitz.Matrix(args.quality, args.quality)
                    pix = page.getPixmap(matrix=mat, alpha=False)
                    pix.writeImage(
                        os.path.join(target_path,
                                     f'{fname}-page{page.number + 1}.jpg'))

            else:
                pix = doc[0].getPixmap()
                pix.writeImage(os.path.join(target_path, f'{fname}.jpg'))


if __name__ == '__main__':
    pdfs = collect_pdf_for_convert(args.input_dir)
    with Pool(processes=None) as pool:
        pool.map(saving_separate_pages, pdfs)
    logger.info('all tasks is done')
