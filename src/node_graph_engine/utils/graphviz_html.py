from __future__ import annotations

import textwrap
from string import Template
from typing import Any
from uuid import uuid4


class GraphvizHTMLProxy:
    """Wrap a graphviz object to provide an HTML representation for docs."""

    def __init__(self, graphviz_obj: Any) -> None:
        self._graphviz_obj = graphviz_obj

    def _repr_html_(self) -> str:
        """Return SVG markup suitable for inclusion in HTML documentation."""

        try:
            from graphviz import backend as graphviz_backend
        except ImportError:
            return "<pre>Graphviz Python package is unavailable.</pre>"

        try:
            svg = self._graphviz_obj.pipe(format="svg")
        except (
            graphviz_backend.ExecutableNotFound,
            graphviz_backend.CalledProcessError,
        ):
            return "<pre>Graphviz rendering is unavailable.</pre>"

        svg_markup = svg.decode("utf-8") if isinstance(svg, bytes) else svg
        container_id = f"provenance-graph-{uuid4().hex}"
        button_id = f"{container_id}-toggle"
        style_id = "provenance-graph-style"

        html_template = textwrap.dedent(
            """
            <div class="provenance-graph-wrapper" id="$container_id">
              <div class="provenance-graph-toolbar">
                <div class="provenance-graph-zoom">
                  <button
                    type="button"
                    class="provenance-graph-toolbar__btn provenance-graph-zoom__btn"
                    data-action="zoom-out"
                    aria-label="Zoom out"
                    title="Zoom out"
                  >−</button>
                  <span class="provenance-graph-zoom__value" aria-live="polite">100%</span>
                  <button
                    type="button"
                    class="provenance-graph-toolbar__btn provenance-graph-zoom__btn"
                    data-action="zoom-in"
                    aria-label="Zoom in"
                    title="Zoom in"
                  >+</button>
                </div>
                <button type="button" class="provenance-graph-toolbar__btn" id="$button_id">Full screen</button>
              </div>
              <div class="provenance-graph-svg">
                $svg_markup
              </div>
            </div>
            <script type="text/javascript">
            (function() {
              var container = document.getElementById("$container_id");
              if (!container) return;
              var button = document.getElementById("$button_id");
              if (!button) return;
              var svgHolder = container.querySelector(".provenance-graph-svg");
              if (!svgHolder) return;
              var svgElement = svgHolder.querySelector("svg");
              if (!svgElement) return;

              var zoomIn = container.querySelector('[data-action="zoom-in"]');
              var zoomOut = container.querySelector('[data-action="zoom-out"]');
              var zoomValue = container.querySelector('.provenance-graph-zoom__value');
              if (!zoomIn || !zoomOut || !zoomValue) return;

              var canvas = document.createElement("div");
              canvas.className = "provenance-graph-canvas";
              svgHolder.appendChild(canvas);
              canvas.appendChild(svgElement);

              var ZOOM_STEP = 0.2;
              var MIN_ZOOM = 0.5;
              var MAX_ZOOM = 3;
              var zoom = 1;

              var updateZoomState = function() {
                canvas.style.transform = "scale(" + zoom + ")";
                zoomValue.textContent = Math.round(zoom * 100) + "%";
                zoomOut.disabled = zoom <= MIN_ZOOM;
                zoomIn.disabled = zoom >= MAX_ZOOM;
              };

              var setZoom = function(newZoom) {
                var nextZoom = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, newZoom));
                if (Math.abs(nextZoom - zoom) < 1e-6) return;
                zoom = nextZoom;
                updateZoomState();
              };

              zoomOut.addEventListener("click", function() {
                setZoom(zoom - ZOOM_STEP);
              });

              zoomIn.addEventListener("click", function() {
                setZoom(zoom + ZOOM_STEP);
              });

              updateZoomState();

              if (!document.getElementById("$style_id")) {
                var style = document.createElement("style");
                style.id = "$style_id";
                style.textContent = `
                  .provenance-graph-wrapper {
                    position: relative;
                    border: 1px solid #d9d9d9;
                    border-radius: 6px;
                    padding: 0.5rem;
                    background-color: #fff;
                    margin: 0 0 1rem 0;
                    box-shadow: 0 1px 2px rgba(15, 23, 42, 0.08);
                  }
                  .provenance-graph-toolbar {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 0.5rem;
                    gap: 0.5rem;
                    flex-wrap: wrap;
                  }
                  .provenance-graph-toolbar__btn {
                    font: inherit;
                    cursor: pointer;
                    color: #1f2937;
                    background-color: #f3f4f6;
                    border: 1px solid #d1d5db;
                    border-radius: 4px;
                    padding: 0.35rem 0.75rem;
                  }
                  .provenance-graph-toolbar__btn:hover {
                    background-color: #e5e7eb;
                  }
                  .provenance-graph-zoom {
                    display: inline-flex;
                    align-items: center;
                    gap: 0.35rem;
                  }
                  .provenance-graph-zoom__btn {
                    width: 2rem;
                    height: 2rem;
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                    padding: 0;
                    font-weight: 600;
                  }
                  .provenance-graph-zoom__btn[disabled] {
                    opacity: 0.4;
                    cursor: not-allowed;
                  }
                  .provenance-graph-zoom__value {
                    min-width: 3ch;
                    text-align: center;
                    font-variant-numeric: tabular-nums;
                  }
                  .provenance-graph-svg {
                    overflow: auto;
                    max-height: 65vh;
                  }
                  .provenance-graph-canvas {
                    display: inline-block;
                    transform-origin: top left;
                  }
                  .provenance-graph-wrapper.is-fullscreen {
                    position: fixed;
                    inset: 0;
                    margin: 0;
                    padding: 1rem;
                    z-index: 9999;
                    background: rgba(15, 23, 42, 0.85);
                    backdrop-filter: blur(2px);
                  }
                  .provenance-graph-wrapper.is-fullscreen .provenance-graph-svg {
                    height: 100%;
                  }
                  .provenance-graph-wrapper.is-fullscreen svg {
                    height: 100%;
                    width: 100%;
                  }
                `;
                document.head.appendChild(style);
              }

              var exitHandler = function() {
                var active = document.fullscreenElement === container ||
                  document.webkitFullscreenElement === container;
                if (active) {
                  container.classList.add("is-fullscreen");
                  button.textContent = "Exit full screen";
                } else {
                  container.classList.remove("is-fullscreen");
                  button.textContent = "Full screen";
                }
              };

              document.addEventListener("fullscreenchange", exitHandler);
              document.addEventListener("webkitfullscreenchange", exitHandler);

              var openInNewTab = function() {
                var svgContent = svgHolder.innerHTML;
                var win = window.open();
                if (win) {
                  win.document.write(
                    "<html><head><title>Provenance graph</title></head>" +
                    "<body style='margin:0'>" + svgContent + "</body></html>"
                  );
                  win.document.close();
                }
              };

              button.addEventListener("click", function() {
                var request = container.requestFullscreen || container.webkitRequestFullscreen;
                if (!document.fullscreenElement && request) {
                  request.call(container);
                } else if (document.fullscreenElement === container || document.webkitFullscreenElement === container) {
                  if (document.exitFullscreen) {
                    document.exitFullscreen();
                  } else if (document.webkitExitFullscreen) {
                    document.webkitExitFullscreen();
                  }
                } else {
                  openInNewTab();
                }
              });

              updateZoomState();
              exitHandler();
            })();
            </script>
            """
        ).strip()

        return Template(html_template).substitute(
            container_id=container_id,
            button_id=button_id,
            style_id=style_id,
            svg_markup=svg_markup,
        )

    def __getattr__(self, item: str) -> Any:
        return getattr(self._graphviz_obj, item)
