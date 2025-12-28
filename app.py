# app.py
# Simple Flask web UI to upload an Excel, run process_prep.py and download the resulting CSV.
# Paste this whole file into your repo's app.py.

import os
import tempfile
import subprocess
from flask import Flask, request, send_file, render_template_string, redirect, url_for, flash

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "change-me-for-prod")

# Simple upload form (no JS)
INDEX_HTML = """
<!doctype html>
<title>Auto-accept prep rules — upload</title>
<h2>Upload Excel (order-level)</h2>
<form method=post enctype=multipart/form-data>
  <label>Venue filter (optional): <input name=venue placeholder="venue name"></label><br/><br/>
  <input type=file name=file accept=".xlsx,.xls" required>
  <p></p>
  <button type=submit>Upload & Generate CSV</button>
</form>
<hr/>
<p>Notes:</p>
<ul>
  <li>The server runs <code>process_prep.py --input INPUT --out OUTPUT</code> and returns OUTPUT.</li>
  <li>Make sure dependencies (pandas/numpy/openpyxl) are installed in the environment.</li>
</ul>
"""

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return render_template_string(INDEX_HTML)

    # POST: handle upload
    if "file" not in request.files:
        flash("No file part")
        return redirect(request.url)

    f = request.files["file"]
    if f.filename == "":
        flash("No selected file")
        return redirect(request.url)

    venue = request.form.get("venue") or None

    # Save uploaded file to tmp
    with tempfile.TemporaryDirectory() as d:
        input_path = os.path.join(d, "input.xlsx")
        out_path = os.path.join(d, "output.csv")

        f.save(input_path)

        # Build command to call process_prep.py
        # Example: python process_prep.py --input input.xlsx --out output.csv --venue "name"
        cmd = ["python3", "process_prep.py", "--input", input_path, "--out", out_path]
        if venue:
            cmd += ["--venue", venue]

        try:
            proc = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=120)
        except subprocess.CalledProcessError as e:
            # Return error output for debugging
            return (
                "<h3>Processing failed (process_prep.py returned non-zero)</h3>"
                f"<pre>stdout:\n{e.stdout}\n\nstderr:\n{e.stderr}</pre>"
            ), 500
        except subprocess.TimeoutExpired:
            return "<h3>Processing timed out (120s)</h3>", 500

        # Ensure output file exists
        if not os.path.exists(out_path):
            return "<h3>process_prep.py did not create output CSV.</h3>", 500

        # Send file back for download
        return send_file(
            out_path,
            as_attachment=True,
            download_name="prep_rules_output.csv",
            mimetype="text/csv",
        )

if __name__ == "__main__":
    # For local dev only. Use gunicorn/uvicorn for production.
    app.run(host="0.0.0.0", port=5000, debug=True)
