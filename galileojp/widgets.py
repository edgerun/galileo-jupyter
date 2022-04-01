import ipywidgets as widgets

from galileojp.frames import ExperimentFrameGateway


def experiment_dropdown(fgw: ExperimentFrameGateway, **kwargs):
    options = [(f'{row[1]}: {row[0]}', row[1]) for row in fgw.experiments()[['NAME', 'EXP_ID']].values]

    return widgets.Dropdown(
        options=options,
        description='Experiment:',
        disabled=False,
        layout=widgets.Layout(width='100%'),
        **kwargs
    )


def experiment_selector(fgw: ExperimentFrameGateway, **kwargs):
    options = [(f'{row[1]}: {row[0]}', row[1]) for row in fgw.experiments()[['NAME', 'EXP_ID']].values]

    return widgets.SelectMultiple(
        options=options,
        description='Experiment:',
        layout=widgets.Layout(width='100%'),
        **kwargs
    )
