import os
import uuid
import logging
import argparse
import fitz
from time import time
from pathlib import Path
from multiprocessing import Pool

logging.basicConfig(level=logging.WARNING)

parser = argparse.ArgumentParser()
parser.add_argument('input_dir', help='specify input dir path for convert')
parser.add_argument('-out', '--output_dir',
                    help='specify output dir path for save jpg')
parser.add_argument('-f', '--flat', action='store_true',
                    help='convert to output_dir without dirs hierarchy')
parser.add_argument('-q', '--quality', type=float, default=2,
                    help='specify convert quality')
parser.add_argument('-p', '--processes', type=int,
                    help='specify number of processor cores')
args = parser.parse_args()


class PDFExtractorError(Exception):
    def __init__(self):
        self.message = 'can\'t load images, check input dir'


def collect_pdf_for_convert(input_dir):
    pdfs = []
    for (root, dirs, files) in os.walk(input_dir, topdown=True):
        for file in files:
            if file.split('.')[-1].lower() == 'pdf':
                pdfs.append(os.path.join(root, file))

    if not pdfs:
        raise PDFExtractorError()

    return pdfs


def saving_img_pages(file, target_path, postfix=None):
    fname = str(Path(file).stem)
    try:
        with open(file, 'rb') as f:
            buf = f.read()
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
                    pix.writeImage(
                        os.path.join(target_path, f'{new_fname}.jpg'))

            return None
    except (RuntimeError, IndexError) as err:
        return f'error: {err}, file: {str(Path(file))}\n'


def convert_with_hierarchy(file):
    print(f' converting {file}')
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

    return saving_img_pages(file, target_path)


def convert_without_hierarchy(file):
    print(f' converting {file}')

    if not args.output_dir:
        target_path = os.path.join(*Path(file).parts[:2])
    else:
        target_path = args.output_dir

    if not os.path.exists(target_path):
        os.makedirs(target_path, exist_ok=True)

    return saving_img_pages(file, target_path, postfix=True)


if __name__ == '__main__':
    start = time()

    if os.path.exists('errors.log'):
        os.remove('errors.log')

    try:
        pdfs_ = collect_pdf_for_convert(args.input_dir)
    except PDFExtractorError as e:
        logging.error(e.message)
    else:
        processes = args.processes if args.processes else None
        with Pool(processes=processes) as pool:
            if args.flat:
                errors = pool.map(convert_without_hierarchy, pdfs_)
            else:
                errors = pool.map(convert_with_hierarchy, pdfs_)

            errors = [e for e in errors if e is not None]

            if errors:
                with open('errors.log', 'w', encoding='utf-8') as log:
                    log.writelines(errors)
                logging.warning(f'{"".join(errors)}')
                logging.warning('errors was found, see errors.log')

        print(f'elapsed time is {int((time() - start) * 100) / 100} sec')
    finally:
        print('all tasks is done')
