"""
Interactive visualization utilities using Plotly.js for data profiling.

Generates interactive charts for:
- Distribution analysis (histogram + KDE, box plots, violin plots)
- Q-Q plots for normality assessment
- Correlation heatmaps with significance indicators
- Anomaly detection visualizations

All charts use Plotly.js JSON specification format for embedding in HTML reports.
"""

import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class PlotlyChartGenerator:
    """Generate Plotly.js chart specifications for data profiling."""

    # DataK9 color scheme
    COLORS = {
        'primary': '#4A90E2',      # K9 Blue
        'secondary': '#FF8C42',    # Guard Orange
        'success': '#2ECC71',      # Green
        'warning': '#F39C12',      # Yellow
        'danger': '#E74C3C',       # Red
        'normal': '#3498DB',       # Light Blue
        'outlier': '#E74C3C',      # Red
        'grid': '#ECF0F1',         # Light Gray
        'text': '#2C3E50'          # Dark Gray
    }

    @staticmethod
    def create_distribution_histogram(
        numeric_values: List[float],
        column_name: str,
        bin_count: int = 50,
        show_kde: bool = True
    ) -> Dict[str, Any]:
        """
        Create interactive histogram with optional KDE overlay.

        Args:
            numeric_values: List of numeric values
            column_name: Column name for title
            bin_count: Number of bins for histogram
            show_kde: Whether to show KDE overlay

        Returns:
            Plotly.js chart specification dict
        """
        if not numeric_values or len(numeric_values) < 2:
            return {}

        try:
            data = []

            # Histogram trace
            histogram_trace = {
                'type': 'histogram',
                'x': numeric_values,
                'nbinsx': bin_count,
                'name': 'Frequency',
                'marker': {
                    'color': PlotlyChartGenerator.COLORS['primary'],
                    'line': {'color': 'white', 'width': 1}
                },
                'opacity': 0.7,
                'hovertemplate': 'Range: %{x}<br>Count: %{y}<extra></extra>'
            }
            data.append(histogram_trace)

            # KDE overlay (if requested and scipy available)
            if show_kde:
                try:
                    from scipy import stats
                    kde = stats.gaussian_kde(numeric_values)
                    x_range = np.linspace(min(numeric_values), max(numeric_values), 200)
                    kde_values = kde(x_range)

                    # Scale KDE to match histogram height
                    hist, bins = np.histogram(numeric_values, bins=bin_count)
                    max_hist = max(hist)
                    max_kde = max(kde_values)
                    if max_kde > 0:
                        kde_values = kde_values * (max_hist / max_kde) * 0.9

                    kde_trace = {
                        'type': 'scatter',
                        'x': x_range.tolist(),
                        'y': kde_values.tolist(),
                        'mode': 'lines',
                        'name': 'KDE',
                        'line': {
                            'color': PlotlyChartGenerator.COLORS['secondary'],
                            'width': 3
                        },
                        'yaxis': 'y2',
                        'hovertemplate': 'Value: %{x:.2f}<br>Density: %{y:.4f}<extra></extra>'
                    }
                    data.append(kde_trace)
                except ImportError:
                    logger.debug("scipy not available - KDE overlay disabled")
                except Exception as e:
                    logger.debug(f"KDE calculation failed: {e}")

            # Layout
            layout = {
                'title': {
                    'text': f'Distribution: {column_name}',
                    'font': {'size': 16, 'color': PlotlyChartGenerator.COLORS['text']}
                },
                'xaxis': {
                    'title': 'Value',
                    'gridcolor': PlotlyChartGenerator.COLORS['grid']
                },
                'yaxis': {
                    'title': 'Frequency',
                    'gridcolor': PlotlyChartGenerator.COLORS['grid']
                },
                'yaxis2': {
                    'title': 'Density',
                    'overlaying': 'y',
                    'side': 'right',
                    'showgrid': False
                } if show_kde else {},
                'plot_bgcolor': 'white',
                'hovermode': 'closest',
                'showlegend': True,
                'legend': {'x': 0.7, 'y': 0.95}
            }

            return {'data': data, 'layout': layout, 'config': {'responsive': True}}

        except Exception as e:
            logger.warning(f"Failed to create histogram for {column_name}: {e}")
            return {}

    @staticmethod
    def create_box_plot(
        numeric_values: List[float],
        column_name: str,
        outliers: Optional[List[float]] = None
    ) -> Dict[str, Any]:
        """
        Create interactive box plot with outlier highlighting.

        Args:
            numeric_values: List of numeric values
            column_name: Column name for title
            outliers: Optional list of outlier values to highlight

        Returns:
            Plotly.js chart specification dict
        """
        if not numeric_values or len(numeric_values) < 2:
            return {}

        try:
            data = []

            # Box plot trace
            box_trace = {
                'type': 'box',
                'y': numeric_values,
                'name': column_name,
                'marker': {
                    'color': PlotlyChartGenerator.COLORS['primary'],
                    'outliercolor': PlotlyChartGenerator.COLORS['outlier']
                },
                'boxmean': 'sd',  # Show mean and standard deviation
                'hovertemplate': (
                    'Max: %{max}<br>'
                    'Q3: %{q3}<br>'
                    'Median: %{median}<br>'
                    'Q1: %{q1}<br>'
                    'Min: %{min}<extra></extra>'
                )
            }
            data.append(box_trace)

            # Layout
            layout = {
                'title': {
                    'text': f'Box Plot: {column_name}',
                    'font': {'size': 16, 'color': PlotlyChartGenerator.COLORS['text']}
                },
                'yaxis': {
                    'title': 'Value',
                    'gridcolor': PlotlyChartGenerator.COLORS['grid']
                },
                'plot_bgcolor': 'white',
                'hovermode': 'closest',
                'showlegend': False
            }

            return {'data': data, 'layout': layout, 'config': {'responsive': True}}

        except Exception as e:
            logger.warning(f"Failed to create box plot for {column_name}: {e}")
            return {}

    @staticmethod
    def create_qq_plot(
        numeric_values: List[float],
        column_name: str,
        distribution_name: str = 'normal'
    ) -> Dict[str, Any]:
        """
        Create Q-Q plot for assessing distribution fit.

        Args:
            numeric_values: List of numeric values
            column_name: Column name for title
            distribution_name: Theoretical distribution name

        Returns:
            Plotly.js chart specification dict
        """
        if not numeric_values or len(numeric_values) < 3:
            return {}

        try:
            from scipy import stats

            # Standardize data
            arr = np.array(numeric_values)
            standardized = (arr - np.mean(arr)) / np.std(arr)
            standardized_sorted = np.sort(standardized)

            # Theoretical quantiles
            n = len(standardized_sorted)
            theoretical_quantiles = stats.norm.ppf(np.linspace(0.01, 0.99, n))

            data = []

            # Q-Q points
            qq_trace = {
                'type': 'scatter',
                'x': theoretical_quantiles.tolist(),
                'y': standardized_sorted.tolist(),
                'mode': 'markers',
                'name': 'Sample Quantiles',
                'marker': {
                    'color': PlotlyChartGenerator.COLORS['primary'],
                    'size': 4,
                    'opacity': 0.6
                },
                'hovertemplate': 'Theoretical: %{x:.2f}<br>Sample: %{y:.2f}<extra></extra>'
            }
            data.append(qq_trace)

            # Reference line (y = x)
            ref_line = {
                'type': 'scatter',
                'x': [-4, 4],
                'y': [-4, 4],
                'mode': 'lines',
                'name': 'Perfect Fit',
                'line': {
                    'color': PlotlyChartGenerator.COLORS['danger'],
                    'width': 2,
                    'dash': 'dash'
                },
                'hoverinfo': 'skip'
            }
            data.append(ref_line)

            # Layout
            layout = {
                'title': {
                    'text': f'Q-Q Plot: {column_name} vs {distribution_name.capitalize()}',
                    'font': {'size': 16, 'color': PlotlyChartGenerator.COLORS['text']}
                },
                'xaxis': {
                    'title': 'Theoretical Quantiles',
                    'gridcolor': PlotlyChartGenerator.COLORS['grid'],
                    'zeroline': True
                },
                'yaxis': {
                    'title': 'Sample Quantiles',
                    'gridcolor': PlotlyChartGenerator.COLORS['grid'],
                    'zeroline': True
                },
                'plot_bgcolor': 'white',
                'hovermode': 'closest',
                'showlegend': True,
                'legend': {'x': 0.05, 'y': 0.95}
            }

            return {'data': data, 'layout': layout, 'config': {'responsive': True}}

        except ImportError:
            logger.debug("scipy not available - Q-Q plot disabled")
            return {}
        except Exception as e:
            logger.warning(f"Failed to create Q-Q plot for {column_name}: {e}")
            return {}

    @staticmethod
    def create_correlation_heatmap(
        correlation_matrix: Dict[str, Dict[str, float]],
        significance_matrix: Optional[Dict[str, Dict[str, float]]] = None,
        method: str = 'pearson'
    ) -> Dict[str, Any]:
        """
        Create interactive correlation heatmap with optional significance indicators.

        Args:
            correlation_matrix: Dict mapping column pairs to correlation coefficients
            significance_matrix: Optional dict mapping column pairs to p-values
            method: Correlation method name (for title)

        Returns:
            Plotly.js chart specification dict
        """
        if not correlation_matrix:
            return {}

        try:
            # Extract column names and build matrix
            columns = sorted(set(
                list(correlation_matrix.keys()) +
                [col for subdict in correlation_matrix.values() for col in subdict.keys()]
            ))

            if len(columns) < 2:
                return {}

            # Build correlation matrix
            z_values = []
            for col1 in columns:
                row = []
                for col2 in columns:
                    if col1 == col2:
                        corr = 1.0
                    elif col1 in correlation_matrix and col2 in correlation_matrix[col1]:
                        corr = correlation_matrix[col1][col2]
                    elif col2 in correlation_matrix and col1 in correlation_matrix[col2]:
                        corr = correlation_matrix[col2][col1]
                    else:
                        corr = 0.0
                    row.append(corr)
                z_values.append(row)

            # Build annotation text (include significance stars)
            annotations = []
            if significance_matrix:
                for i, col1 in enumerate(columns):
                    for j, col2 in enumerate(columns):
                        corr = z_values[i][j]
                        text = f'{corr:.2f}'

                        # Add significance stars
                        p_value = None
                        if col1 in significance_matrix and col2 in significance_matrix[col1]:
                            p_value = significance_matrix[col1][col2]
                        elif col2 in significance_matrix and col1 in significance_matrix[col2]:
                            p_value = significance_matrix[col2][col1]

                        if p_value is not None:
                            if p_value < 0.001:
                                text += '***'
                            elif p_value < 0.01:
                                text += '**'
                            elif p_value < 0.05:
                                text += '*'

                        annotations.append({
                            'x': j,
                            'y': i,
                            'text': text,
                            'showarrow': False,
                            'font': {
                                'color': 'white' if abs(corr) > 0.5 else 'black',
                                'size': 10
                            }
                        })

            data = [{
                'type': 'heatmap',
                'z': z_values,
                'x': columns,
                'y': columns,
                'colorscale': [
                    [0, '#E74C3C'],      # Red (negative)
                    [0.5, '#ECF0F1'],    # White (zero)
                    [1, '#2ECC71']       # Green (positive)
                ],
                'zmid': 0,
                'zmin': -1,
                'zmax': 1,
                'colorbar': {
                    'title': 'Correlation',
                    'tickvals': [-1, -0.5, 0, 0.5, 1],
                    'ticktext': ['-1.0', '-0.5', '0.0', '0.5', '1.0']
                },
                'hovertemplate': '%{x} vs %{y}<br>Correlation: %{z:.3f}<extra></extra>'
            }]

            layout = {
                'title': {
                    'text': f'Correlation Matrix ({method.capitalize()})',
                    'font': {'size': 16, 'color': PlotlyChartGenerator.COLORS['text']}
                },
                'xaxis': {
                    'tickangle': -45,
                    'side': 'bottom'
                },
                'yaxis': {
                    'tickangle': 0
                },
                'annotations': annotations if significance_matrix else [],
                'plot_bgcolor': 'white',
                'width': 600,
                'height': 600
            }

            return {'data': data, 'layout': layout, 'config': {'responsive': True}}

        except Exception as e:
            logger.warning(f"Failed to create correlation heatmap: {e}")
            return {}

    @staticmethod
    def create_value_frequency_chart(
        top_values: List[Dict[str, Any]],
        column_name: str,
        max_values: int = 20
    ) -> Dict[str, Any]:
        """
        Create horizontal bar chart for top value frequencies.

        Args:
            top_values: List of dicts with 'value' and 'count' keys
            column_name: Column name for title
            max_values: Maximum number of values to display

        Returns:
            Plotly.js chart specification dict
        """
        if not top_values:
            return {}

        try:
            # Limit to max_values
            values_to_plot = top_values[:max_values]

            # Extract values and counts
            labels = [str(item.get('value', '')) for item in values_to_plot]
            counts = [item.get('count', 0) for item in values_to_plot]

            # Reverse for better display (highest at top)
            labels = labels[::-1]
            counts = counts[::-1]

            data = [{
                'type': 'bar',
                'x': counts,
                'y': labels,
                'orientation': 'h',
                'marker': {
                    'color': PlotlyChartGenerator.COLORS['primary'],
                    'line': {'color': 'white', 'width': 1}
                },
                'hovertemplate': '%{y}: %{x:,} occurrences<extra></extra>'
            }]

            layout = {
                'title': {
                    'text': f'Top {len(values_to_plot)} Values: {column_name}',
                    'font': {'size': 16, 'color': PlotlyChartGenerator.COLORS['text']}
                },
                'xaxis': {
                    'title': 'Count',
                    'gridcolor': PlotlyChartGenerator.COLORS['grid']
                },
                'yaxis': {
                    'title': 'Value',
                    'automargin': True
                },
                'plot_bgcolor': 'white',
                'hovermode': 'closest',
                'showlegend': False,
                'height': max(400, len(values_to_plot) * 25)
            }

            return {'data': data, 'layout': layout, 'config': {'responsive': True}}

        except Exception as e:
            logger.warning(f"Failed to create value frequency chart for {column_name}: {e}")
            return {}

    @staticmethod
    def render_chart_html(chart_spec: Dict[str, Any], div_id: str) -> str:
        """
        Generate HTML code to render a Plotly chart.

        Args:
            chart_spec: Plotly chart specification dict
            div_id: Unique div ID for the chart

        Returns:
            HTML string with JavaScript to render the chart
        """
        if not chart_spec:
            return ""

        import json

        try:
            chart_json = json.dumps(chart_spec)

            html = f"""
            <div id="{div_id}" style="width: 100%; height: 100%;"></div>
            <script>
                (function() {{
                    var spec = {chart_json};
                    Plotly.newPlot('{div_id}', spec.data, spec.layout, spec.config);
                }})();
            </script>
            """

            return html

        except Exception as e:
            logger.warning(f"Failed to render chart HTML: {e}")
            return ""
