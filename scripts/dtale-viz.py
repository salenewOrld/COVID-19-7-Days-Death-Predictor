from multiprocessing.sharedctypes import Value
from flask import redirect
import pandas as pd
import dtale
from dtale.app import build_app
from dtale.views import startup
import sys
if __name__ == '__main__':
    app = build_app(reaper_on=False)
    try:
        file = f'/usr/src/{sys.argv[1]}'
    except:
        raise ValueError("The following variable(s) not provided by user:\n- FILE_PATH")
    #@app.route("/visualization")
    @app.route("/")
    def hello_world():
        df = pd.read_csv(file)
        instance = startup(data=df, ignore_duplicate=True)
        return redirect(f"/dtale/main/{instance._data_id}", code=302)

    app.run(host="0.0.0.0", port=8181)