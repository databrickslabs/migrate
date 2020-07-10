import click as click
import click_log

from databricks_migrate import CONTEXT_SETTINGS, log
from databricks_migrate.db_export import export_cli
from databricks_migrate.db_import import import_cli
from databricks_migrate.db_test_connection import test_connection_cli
from databricks_migrate.version import print_version_callback, version


@click.group(CONTEXT_SETTINGS)
@click.option('--version', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@click_log.simple_verbosity_option(log, '--verbosity', '-v')
def cli():
    pass

cli.add_command(import_cli, name='import')
cli.add_command(export_cli, name='export')
cli.add_command(test_connection_cli, name='test_connection')

if __name__ == "__main__":
    cli()