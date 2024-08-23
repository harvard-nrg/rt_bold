import sys
import redis
import random
import logging
import pandas as pd
from pubsub import pub
import plotly.express as px
from dash import Dash, html, dcc, callback, Output, Input, State

logger = logging.getLogger(__name__)

ERROR_ART = """
██╗    ██╗ █████╗ ██████╗ ███╗   ██╗██╗███╗   ██╗ ██████╗ 
██║    ██║██╔══██╗██╔══██╗████╗  ██║██║████╗  ██║██╔════╝ 
██║ █╗ ██║███████║██████╔╝██╔██╗ ██║██║██╔██╗ ██║██║  ███╗
██║███╗██║██╔══██║██╔══██╗██║╚██╗██║██║██║╚██╗██║██║   ██║
╚███╔███╔╝██║  ██║██║  ██║██║ ╚████║██║██║ ╚████║╚██████╔╝
 ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝╚═╝  ╚═══╝ ╚═════╝ 
"""
class View:
    def __init__(self, host='127.0.0.1', port=8080):
        logger.info('constructing dash view')
        self._app = Dash('Realtime fMRI is fun')
        self._host = host
        self._port = port
        self._title = 'Realtime fMRI Motion'
        self._subtitle = ''
        self._instances = dict()
        self._debug = False
        self._redis_client = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
        self.init_page()
        self.init_callbacks()
        pub.subscribe(self.listener, 'plot')

    def init_page(self):
        self._app.layout = html.Div([
            html.H2(
                id='graph-title',
                children=self._title,
                style={
                    'textAlign': 'center'
                }
            ),
            html.H3(
                id='sub-title',
                children=self._subtitle,
                style={
                    'textAlign': 'center'
                }
            ),
            dcc.Graph(id='live-update-displacements'),
            dcc.Graph(id='live-update-rotations'),
            html.Dialog(
                id='warning-dialog',
                children=[
                    html.Pre(
                        ERROR_ART,
                        id='warning-title',
                        style={
                            'color': 'red',
                            'verticalAlign': 'center',
                            'fontFamily': 'courier',
                            'fontSize': '2.5vh',
                            'fontWeight': 'bold',
                            'padding': '5vh 5vw 5vh 5vw',
                            #'border': '1px solid red'
                        }
                    ),
                    html.Pre(
                        id='warning-content',
                        style={
                            'color': 'red',
                            'fontFamily': 'courier',
                            'fontSize': '3vh',
                            'textAlign': 'left',
                            'padding': '5vh 8vw 5vh 8vw',
                            #'border': '1px solid red',
                        }
                    ),
                    html.Button(
                        'DISMISS',
                        id='close-warning-button',
                        n_clicks=0,
                        style={
                            'color': 'black',
                            'borderColor': 'grey',
                            'borderWidth': '1vh',
                            'backgroundColor': '',
                            'padding': '1vh 1vw 1vh 1vw',
                            'fontFamily': 'courier',
                            'fontSize': '3vh'
                        }
                    )
                ],
                style={
                    'backgroundColor': 'black',
                    'position': 'absolute',
                    'top': 0,
                    'height': '100vh',
                    'width': '100vw',
                    'padding': 0,
                    'margin': 0,
                    'textAlign': 'center',
                }

            ),
            dcc.Interval(
                id='interval-component',
                interval=1 * 1000
            ),
            dcc.Interval(
                id='warning-interval-component',
                interval=1 * 1000
            ),
           dcc.Store(id='warning-message-store', data={'visible': False, 'content': ''})
        ])


    def init_callbacks(self):
        self._app.callback(
            Output('live-update-displacements', 'figure'),
            Output('live-update-rotations', 'figure'),
            Output('sub-title', 'children'),
            Input('interval-component', 'n_intervals'),
        )(self.update_graphs)

        self._app.callback(
            Output('warning-message-store', 'data', allow_duplicate=True),
            Input('warning-interval-component', 'n_intervals'),
            prevent_initial_call=True
        )(self.check_redis_for_warnings)    

        self._app.callback(
            Output('warning-dialog', 'open', allow_duplicate=True),
            Output('warning-content', 'children'),
            Input('warning-message-store', 'data'),
            prevent_initial_call=True
        )(self.warning_display) 

        self._app.callback(
            Output('warning-dialog', 'open'),
            Output('warning-message-store', 'data'),
            Input('close-warning-button', 'n_clicks'),
            State('warning-message-store', 'data'),
            prevent_initial_call=True
        )(self.close_warning)

    def warning_display(self, stored_data):
        if stored_data['visible']:
            open = True
            warning_content = stored_data['content']
        else:
            open = False
            warning_content = '' 
        return open, warning_content

    def check_redis_for_warnings(self, n):
        message = self._redis_client.get('scanbuddy_messages')
        if message:
            logger.debug('message found, showing warning screen')
            return {'visible': True, 'content': message.decode('utf-8')}
        return {'visible': False, 'content': ''}

    def close_warning(self, n_clicks, stored_data):
        if n_clicks is not None and n_clicks > 0:
            logger.debug('warning screen closed by user, deleting redis entry')
            self._redis_client.delete('scanbuddy_messages')
            return False, {'visible': False, 'content': ''}
        return stored_data

    def update_graphs(self, n):
        df = self.todataframe()
        disps = self.displacements(df)
        rots = self.rotations(df)
        title = self.get_subtitle()
        return disps,rots,title

    def get_subtitle(self):
        title = self._subtitle
        return title

    def displacements(self, df):
        fig = px.line(df, x='N', y=['x', 'y', 'z'])
        fig.update_layout(
            title={
                'text': 'Translations',
                'x': 0.5
            },
            yaxis_title='mm',
            legend={
                'title': ''
            }
        )
        return fig

    def rotations(self, df):
        fig = px.line(df, x='N', y=['roll', 'pitch', 'yaw'])
        fig.update_layout(
            title={
                'text': 'Rotations',
                'x': 0.5
            },
            yaxis_title='degrees (ccw)',
            legend={
                'title': ''
            }
        )
        return fig

    def todataframe(self):
        arr = list()
        for i,instance in enumerate(self._instances.values(), start=1):
            volreg = instance['volreg']
            if volreg:
                arr.append([i] + volreg)
        df = pd.DataFrame(arr, columns=['N', 'roll', 'pitch', 'yaw', 'x', 'y', 'z'])
        return df

    def forever(self):
        self._app.run(
            host=self._host,
            port=self._port,
            debug=self._debug
        )

    def listener(self, instances, subtitle_string):
        self._instances = instances
        self._subtitle = subtitle_string
