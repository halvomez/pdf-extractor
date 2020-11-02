import argparse
import hashlib
import logging
from multiprocessing import Pool, Lock
from pathlib import Path
from time import time
from typing import List, Union

import fitz

logging.basicConfig(level=logging.WARNING)

parser = argparse.ArgumentParser()
parser.add_argument('input_dir', help='specify input dir path for convert')
parser.add_argument('-out', '--output_dir',
                    help='specify output dir path for save jpg')
parser.add_argument('-f', '--flat', action='store_true',
                    help='convert without save dirs hierarchy')
parser.add_argument('-q', '--quality', type=float, default=2,
                    help='specify convert quality')
parser.add_argument('-p', '--processes', type=int,
                    help='specify number of processor cores')
args = parser.parse_args()


class PDFExtractorError(Exception):
    def __init__(self):
        self.message = 'can\'t load images, check input dir'


def collect_pdfs_for_convert(input_dir: str) -> List[Path]:
    pdfs_ = [file for file in Path(input_dir).glob('**/*.*') if
             file.suffix.lower() == '.pdf']

    if not pdfs_:
        raise PDFExtractorError()

    return pdfs_


def convert_file(file: Path) -> Union[bool, str]:
    print(f' converting {file}')

    try:
        with open(file, 'rb') as f:
            f_content = f.read()
            new_fname = f'{hashlib.blake2b(f_content).hexdigest()[:12]}'

            if args.flat:
                target_dir = Path(
                    args.output_dir if args.output_dir else args.input_dir)
            else:
                fpath_parts = Path(file).parts
                ipath_parts = Path(args.input_dir).parts

                target_dir = Path(args.output_dir) / Path(*fpath_parts[len(
                    ipath_parts):-1]) if args.output_dir else file.parent

            Path(target_dir).mkdir(exist_ok=True)

            with fitz.Document(new_fname, f_content) as doc:
                if doc.pageCount > 1:
                    for page in doc:
                        mat = fitz.Matrix(args.quality, args.quality)
                        pix = page.getPixmap(matrix=mat, alpha=False)
                        page_name = f'{new_fname}-page{page.number + 1}.jpg'
                        with lock:
                            pix.writeImage(str(target_dir / page_name))

                else:
                    pix = doc[0].getPixmap()
                    with lock:
                        pix.writeImage(str(target_dir / f'{new_fname}.jpg'))

        return True
    except (RuntimeError, IndexError) as err:
        return f'error: {err}, file: {file}\n'


def init_lock(l: Lock) -> None:
    global lock
    lock = l


if __name__ == '__main__':
    start = time()

    Path('errors.log').unlink(missing_ok=True)

    try:
        pdfs = collect_pdfs_for_convert(args.input_dir)
    except PDFExtractorError as e:
        logging.error(e.message)
    else:
        lock = Lock()
        pool = Pool(processes=args.processes if args.processes else None,
                    initializer=init_lock, initargs=(lock,))

        results = pool.map(convert_file, pdfs)
        pool.close()
        pool.join()

        errors = [x for x in results if x is not True]

        if errors:
            with open('errors.log', 'w', encoding='utf-8') as log:
                log.writelines(errors)
            logging.warning(f'{"".join(errors)}')
            logging.warning('errors was found, see errors.log')

    print(f'elapsed time is {time() - start:.2f} sec')
    print('all tasks is done')
