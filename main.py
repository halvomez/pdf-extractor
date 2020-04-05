import os
import uuid
import logging
import argparse
import fitz
from time import time
from pathlib import Path
from multiprocessing import Pool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('log')
logger.setLevel('INFO')

parser = argparse.ArgumentParser()
parser.add_argument('input_dir', help='specify input dir path for convert')
parser.add_argument('-out', '--output_dir',
                    help='specify output dir path for save jpg')
parser.add_argument('-f', '--flat', action='store_true',
                    help='convert to output_dir without dirs hierarchy')
parser.add_argument('-q', '--quality', type=float, default=2.5,
                    help='specify convert quality')
args = parser.parse_args()


class PDFWorkerError(Exception):
    def __init__(self):
        self.message = 'can\'t load images, check input dir'


def collect_pdf_for_convert(input_dir):
    _pdfs = []
    for (root, dirs, files) in os.walk(input_dir, topdown=True):
        for file in files:
            if file.split('.')[-1].lower() == 'pdf':
                _pdfs.append(os.path.join(root, file))

    if not _pdfs:
        raise PDFWorkerError()

    return _pdfs


def saving_img_pages(file, target_path, postfix=None):
    fname = str(Path(file).stem)
    with open(file, 'rb') as file_:
        buf = file_.read()
        postfix_ = uuid.uuid4().time_low

        with fitz.Document(fname, buf) as doc:
            new_fname = f'{fname}_{postfix_}' if postfix else fname
            if doc.pageCount > 1:
                for page in doc:
                    mat = fitz.Matrix(args.quality, args.quality)
                    pix = page.getPixmap(matrix=mat, alpha=False)
                    pix.writeImage(
                        os.path.join(target_path,
                                     f'{new_fname}-page{page.number + 1}.jpg'))

            else:
                pix = doc[0].getPixmap()
                pix.writeImage(os.path.join(target_path, f'{new_fname}.jpg'))


def convert_with_hierarchy(file):
    logger.info(f' converting {file}')
    processing_path = str(Path(file).parent)

    fpath_parts = Path(file).parts
    ipath_parts = Path(args.input_dir).parts

    if not args.output_dir:
        target_path = processing_path
    else:
        target_path = os.path.join(args.output_dir,
                                   *fpath_parts[len(ipath_parts):-1])

    if not os.path.exists(target_path):
        os.makedirs(target_path, exist_ok=True)

    saving_img_pages(file, target_path)


def convert_without_hierarchy(file):
    logger.info(f' converting {file}')

    if not args.output_dir:
        target_path = os.path.join(*Path(file).parts[:2])
    else:
        target_path = args.output_dir

    if not os.path.exists(target_path):
        os.makedirs(target_path, exist_ok=True)

    saving_img_pages(file, target_path, postfix=True)


if __name__ == '__main__':
    start = time()
    try:
        pdfs = collect_pdf_for_convert(args.input_dir)
    except PDFWorkerError as e:
        logging.error(e.message)
    else:
        with Pool(processes=None) as pool:
            if args.flat:
                pool.map(convert_without_hierarchy, pdfs)
            else:
                pool.map(convert_with_hierarchy, pdfs)
        logger.info('all tasks is done')
        logger.info(f'elapsed time is {int((time() - start) * 100) / 100} sec')
