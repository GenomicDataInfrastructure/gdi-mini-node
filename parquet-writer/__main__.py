import logging
from argparse import ArgumentParser, Namespace

from _vcf_af_reader import VcfAlleleFreqReader
from _vcf_individuals_reader import VcfIndividualsReader
from _parquet import PQ_VCF_AF_SCHEMA, PQ_VCF_INDIVIDUAL_SCHEMA, \
    ParquetVcfWriter, print_individuals_summary, write_individuals_parquet

_log = logging.getLogger(__name__)


def main() -> int:
    """The main entry point of the parquet-writer program."""

    parser = _build_parser()
    args = parser.parse_args()

    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 1

    log = _configure_logging(args)
    try:
        func(args)
        return 0
    except RuntimeError as e:
        log.debug("Runtime error was raised", exc_info=True)
        log.error("%s", e)
        return 1
    except KeyboardInterrupt:
        log.error("Interrupted by user")
        return 130
    except:
        log.exception("Unhandled error")
        return 1


def cmd_af_parquet(args: Namespace) -> None:
    """Implements the "allele-freq" command."""
    writer = ParquetVcfWriter(args.dest, PQ_VCF_AF_SCHEMA, "allele-freq-chr")
    for vcf_path in args.vcf_paths:
        reader = VcfAlleleFreqReader(vcf_path, args.cohort)
        try:
            reader.write_to(writer)
        finally:
            reader.close()


def cmd_individuals_parquet(args: Namespace) -> None:
    """Implements the "individuals" command."""
    dest_dir = args.dest
    process_individuals_data = True
    parquet_schema = PQ_VCF_INDIVIDUAL_SCHEMA
    writer = ParquetVcfWriter(dest_dir, parquet_schema, "individuals-chr")

    for vcf_path in args.vcf_paths:
        reader = VcfIndividualsReader(vcf_path)

        # There can be multiple VCF files but here we just expect all of them to
        # contain the same samples. So the CSV needs to be processed just once.
        # It could be improved with a check that the samples are the same
        # (including their order) in all input VCF files.

        if process_individuals_data:
            write_individuals_parquet(args.csv_file, dest_dir, reader.samples())
            process_individuals_data = False

        try:
            reader.write_to(writer)
        finally:
            reader.close()


def cmd_individuals_summary(args: Namespace) -> None:
    """Implements the "summary" command."""
    print_individuals_summary(args.parquet_file)


def _configure_logging(args: Namespace):
    level = logging.INFO
    pattern = "%(levelname)-7s [%(filename)s] %(message)s"

    if args.debug_logging:
        level = logging.DEBUG
        pattern = "%(asctime)s %(levelname)-7s [%(filename)s:%(lineno)d] %(message)s"

    logging.basicConfig(level=level, format=pattern)
    return logging.getLogger(__name__)


def _build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog=f"parquet-writer",
        description="Converts one or more VCF files to allele frequency Parquet files.",
    )
    subparsers = parser.add_subparsers(required=True)

    cmd_parser = subparsers.add_parser(
        "allele-freq", help="Create allele frequency parquet files",
    )
    cmd_parser.set_defaults(func=cmd_af_parquet)
    cmd_parser.add_argument(
        "-d", "--dest",
        dest="dest",
        default=".",
        metavar="DIR",
        help="destination directory (defaults to the current directory) for Parquet files",
    )
    cmd_parser.add_argument(
        "-c", "--cohort",
        dest="cohort",
        help="default population label (two-letter country-code) when the VCF does not define any",
    )
    cmd_parser.add_argument(
        "vcf_paths",
        nargs="+",
        metavar="VCF_FILE",
        help="path to one or more VCF files (*.vcf.gz)",
    )
    cmd_parser.add_argument(
        "-v", "--verbose",
        dest="debug_logging",
        action="store_true",
        help="activates more verbose logging",
    )

    cmd_parser = subparsers.add_parser(
        "individuals", help="Create individuals-based parquet files",
    )
    cmd_parser.set_defaults(func=cmd_individuals_parquet)
    cmd_parser.add_argument(
        "-d", "--dest",
        dest="dest",
        default=".",
        metavar="DIR",
        help="destination directory (defaults to the current directory) for Parquet files",
    )
    cmd_parser.add_argument(
        "-i", "--individuals",
        dest="csv_file",
        required=True,
        help="CSV file about the individuals in the VCF files (columns: SAMPLE, SEX, AGE)",
    )
    cmd_parser.add_argument(
        "vcf_paths",
        nargs="+",
        metavar="VCF_FILE",
        help="path to one or more VCF files (*.vcf.gz)",
    )
    cmd_parser.add_argument(
        "-v", "--verbose",
        dest="debug_logging",
        action="store_true",
        help="activates more verbose logging",
    )


    cmd_parser = subparsers.add_parser(
        "summary", help="Print individuals.parquet summary for the metadata.yaml",
    )
    cmd_parser.set_defaults(func=cmd_individuals_summary)
    cmd_parser.add_argument(
        "-i", "--individuals",
        dest="parquet_file",
        required=True,
        help="Reference to an existing individuals.parquet file",
    )
    cmd_parser.add_argument(
        "-v", "--verbose",
        dest="debug_logging",
        action="store_true",
        help="activates more verbose logging",
    )

    return parser

if __name__ == "__main__":
    main()
