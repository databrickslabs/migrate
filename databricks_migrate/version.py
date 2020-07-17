version = '1.0.0'


def print_version_callback(ctx, param, value):
    import click
    if not value or ctx.resilient_parsing:
        return
    click.echo('Version {}'.format(version))
    ctx.exit()
