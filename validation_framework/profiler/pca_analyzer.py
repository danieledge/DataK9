"""
PCA/Dimensionality Reduction Analyzer for DataK9 Profiler.

Provides 2D embeddings of high-dimensional data for visualization and insight.
Uses PCA for numeric-only datasets, with fallback explanations for mixed types.

Key features:
- PCA for numeric data with explained variance metrics
- Feature loadings for interpretation
- Interactive 2D scatter plot with outlier highlighting
- Graceful degradation for insufficient data
"""

import logging
import numpy as np
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


class PCAAnalyzer:
    """
    Analyze datasets using Principal Component Analysis for 2D visualization.

    Provides:
    - 2D PCA projection for visualization
    - Explained variance ratios
    - Feature loadings/contributions
    - Outlier highlighting in reduced space
    """

    MIN_SAMPLES = 10       # Minimum rows for PCA
    MIN_FEATURES = 3       # Minimum numeric columns for meaningful PCA
    MAX_FEATURES = 50      # Limit features to avoid memory issues

    def __init__(self, n_components: int = 2):
        """
        Initialize PCA analyzer.

        Args:
            n_components: Number of principal components (default 2 for visualization)
        """
        self.n_components = n_components
        self.pca = None
        self.scaler = None

    def analyze(
        self,
        df,
        numeric_columns: List[str],
        outlier_indices: Optional[List[int]] = None,
        target_column: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Perform PCA analysis on numeric columns.

        Args:
            df: DataFrame to analyze
            numeric_columns: List of numeric column names
            outlier_indices: Optional list of row indices flagged as outliers
            target_column: Optional target column for coloring points

        Returns:
            Dict with PCA results including:
            - available: Whether PCA was performed
            - projection: 2D coordinates for each row
            - explained_variance: Variance explained by each component
            - loadings: Feature contributions to each component
            - interpretation: Plain English explanation
        """
        result = {
            'available': False,
            'reason': None,
            'projection': None,
            'explained_variance': None,
            'cumulative_variance': None,
            'loadings': None,
            'top_contributors': None,
            'outlier_separation': None,
            'interpretation': None,
            'chart_spec': None
        }

        # Validate inputs
        if df is None or len(df) < self.MIN_SAMPLES:
            result['reason'] = f"Insufficient rows for PCA (need at least {self.MIN_SAMPLES})"
            return result

        # Filter to valid numeric columns
        valid_columns = [c for c in numeric_columns if c in df.columns]
        if len(valid_columns) < self.MIN_FEATURES:
            result['reason'] = f"Insufficient numeric columns for PCA (need at least {self.MIN_FEATURES})"
            return result

        # Limit features if too many
        if len(valid_columns) > self.MAX_FEATURES:
            # Select columns with highest variance
            variances = df[valid_columns].var()
            valid_columns = variances.nlargest(self.MAX_FEATURES).index.tolist()

        try:
            from sklearn.preprocessing import StandardScaler
            from sklearn.decomposition import PCA

            # Prepare data - drop rows with NaN
            data = df[valid_columns].dropna()
            if len(data) < self.MIN_SAMPLES:
                result['reason'] = f"Too many missing values - only {len(data)} complete rows"
                return result

            # Store original indices for outlier mapping
            valid_row_indices = data.index.tolist()

            # Standardize features
            self.scaler = StandardScaler()
            scaled_data = self.scaler.fit_transform(data)

            # Perform PCA
            n_components = min(self.n_components, len(valid_columns), len(data))
            self.pca = PCA(n_components=n_components)
            projection = self.pca.fit_transform(scaled_data)

            # Extract results
            explained_variance = self.pca.explained_variance_ratio_.tolist()
            cumulative_variance = np.cumsum(explained_variance).tolist()

            # Get loadings (feature contributions)
            loadings = {}
            for i, component in enumerate(self.pca.components_):
                loadings[f'PC{i+1}'] = {
                    col: float(loading)
                    for col, loading in zip(valid_columns, component)
                }

            # Identify top contributors for each component
            top_contributors = {}
            for pc_name, pc_loadings in loadings.items():
                sorted_loadings = sorted(pc_loadings.items(), key=lambda x: abs(x[1]), reverse=True)
                top_contributors[pc_name] = sorted_loadings[:5]  # Top 5 contributors

            # Check outlier separation in reduced space
            outlier_separation = None
            if outlier_indices and len(outlier_indices) > 0:
                # Map outlier indices to valid row indices
                outlier_mask = [i in outlier_indices for i in valid_row_indices]
                if any(outlier_mask):
                    outlier_points = projection[outlier_mask]
                    normal_points = projection[[not m for m in outlier_mask]]

                    if len(outlier_points) > 0 and len(normal_points) > 0:
                        # Calculate centroid distances
                        outlier_centroid = outlier_points.mean(axis=0)
                        normal_centroid = normal_points.mean(axis=0)
                        separation_distance = np.linalg.norm(outlier_centroid - normal_centroid)

                        # Calculate average within-group distance for context
                        normal_spread = np.std(normal_points, axis=0).mean() if len(normal_points) > 1 else 1

                        outlier_separation = {
                            'distance': float(separation_distance),
                            'normalized_distance': float(separation_distance / (normal_spread + 1e-10)),
                            'n_outliers': sum(outlier_mask),
                            'separated': separation_distance > 2 * normal_spread
                        }

            # Generate chart specification (needed for target_info in interpretation)
            chart_spec = self._generate_chart_spec(
                projection,
                valid_row_indices,
                outlier_indices or [],
                explained_variance,
                target_column,
                df if target_column else None,
                top_contributors
            )

            # Generate interpretation (after chart_spec to get target_info)
            interpretation = self._generate_interpretation(
                explained_variance,
                top_contributors,
                outlier_separation,
                len(valid_columns),
                target_column,
                chart_spec.get('target_info')
            )

            result.update({
                'available': True,
                'projection': projection.tolist(),
                'row_indices': valid_row_indices,
                'explained_variance': explained_variance,
                'cumulative_variance': cumulative_variance,
                'loadings': loadings,
                'top_contributors': {k: [(col, round(v, 3)) for col, v in v] for k, v in top_contributors.items()},
                'columns_used': valid_columns,
                'n_samples': len(data),
                'outlier_separation': outlier_separation,
                'interpretation': interpretation,
                'chart_spec': chart_spec
            })

        except ImportError:
            result['reason'] = "scikit-learn not available for PCA"
        except Exception as e:
            logger.warning(f"PCA analysis failed: {e}")
            result['reason'] = f"PCA computation failed: {str(e)}"

        return result

    def _generate_interpretation(
        self,
        explained_variance: List[float],
        top_contributors: Dict[str, List[Tuple[str, float]]],
        outlier_separation: Optional[Dict],
        n_features: int,
        target_column: Optional[str] = None,
        target_info: Optional[Dict] = None
    ) -> Dict[str, str]:
        """Generate plain English and technical interpretations."""

        total_variance = sum(explained_variance) * 100
        pc1_variance = explained_variance[0] * 100 if explained_variance else 0
        pc2_variance = explained_variance[1] * 100 if len(explained_variance) > 1 else 0

        # Get main drivers
        pc1_drivers = top_contributors.get('PC1', [])
        pc2_drivers = top_contributors.get('PC2', [])
        main_driver = pc1_drivers[0][0] if pc1_drivers else None

        # Build comprehensive plain English interpretation
        plain_parts = []

        # Start with what the chart shows
        plain_parts.append(
            "This scatter plot shows all records projected into 2D space. "
            "Each dot is one record. Points close together are similar; points far apart are different."
        )

        # Explain the axes
        if main_driver:
            pc1_direction = "higher" if pc1_drivers[0][1] > 0 else "lower"
            plain_parts.append(
                f"Moving right means {pc1_direction} {main_driver} values."
            )

        # Explain coloring based on target
        if target_info and target_info.get('column'):
            target_col = target_info['column']
            values = target_info.get('values', [])
            if len(values) == 2:
                plain_parts.append(
                    f"Colors show the {target_col} values. "
                    f"If colors cluster separately, {target_col} is predictable from other features."
                )
            else:
                plain_parts.append(
                    f"Colors represent different {target_col} categories. "
                    f"Separated color clusters suggest {target_col} can be predicted from the data."
                )

        # Variance quality assessment
        if total_variance >= 80:
            plain_parts.append(
                f"The 2D view captures {total_variance:.0f}% of the data's variation - most patterns are visible here."
            )
        elif total_variance >= 50:
            plain_parts.append(
                f"This captures {total_variance:.0f}% of variation. "
                "Key patterns are shown, but some complexity is hidden in higher dimensions."
            )
        else:
            plain_parts.append(
                f"Only {total_variance:.0f}% of variation is shown. "
                "The data is highly complex - this is a simplified summary."
            )

        # Outlier information
        if outlier_separation:
            if outlier_separation.get('separated'):
                plain_parts.append(
                    f"Detected outliers ({outlier_separation.get('n_outliers', 0)} records) "
                    "appear clearly separated from normal data."
                )
            else:
                plain_parts.append(
                    "Outliers are mixed with normal points, suggesting they differ in ways not captured here."
                )

        plain = " ".join(plain_parts)

        # Technical interpretation
        technical = f"PCA on {n_features} features. "
        technical += f"PC1 explains {pc1_variance:.1f}% variance"
        if len(explained_variance) > 1:
            technical += f", PC2 explains {pc2_variance:.1f}%. "
        else:
            technical += ". "
        technical += f"Cumulative: {total_variance:.1f}%."

        if pc1_drivers:
            technical += f" PC1 loadings: {', '.join([f'{col}({v:+.2f})' for col, v in pc1_drivers[:3]])}."
        if pc2_drivers:
            technical += f" PC2 loadings: {', '.join([f'{col}({v:+.2f})' for col, v in pc2_drivers[:3]])}."

        if target_column:
            technical += f" Colored by: {target_column}."

        return {
            'plain_english': plain,
            'technical': technical
        }

    def _generate_chart_spec(
        self,
        projection: np.ndarray,
        row_indices: List[int],
        outlier_indices: List[int],
        explained_variance: List[float],
        target_column: Optional[str],
        df,
        top_contributors: Optional[Dict[str, List[Tuple[str, float]]]] = None
    ) -> Dict[str, Any]:
        """Generate Plotly.js chart specification for 2D scatter plot."""

        # Create outlier mask
        outlier_mask = [i in outlier_indices for i in row_indices]

        # Prepare point data
        x_data = projection[:, 0].tolist()
        y_data = projection[:, 1].tolist() if projection.shape[1] > 1 else [0] * len(projection)

        # Layout defaults
        pc1_var = explained_variance[0] * 100 if explained_variance else 0
        pc2_var = explained_variance[1] * 100 if len(explained_variance) > 1 else 0

        # Build meaningful axis labels from top contributors
        pc1_contributors = top_contributors.get('PC1', []) if top_contributors else []
        pc2_contributors = top_contributors.get('PC2', []) if top_contributors else []

        def format_axis_label(contributors: List[Tuple[str, float]], pc_num: int, variance: float) -> str:
            """Create axis label showing top features and direction."""
            if not contributors:
                return f'PC{pc_num} ({variance:.0f}%)'

            # Get top 2 contributors with direction indicators
            parts = []
            for col, loading in contributors[:2]:
                direction = '↑' if loading > 0 else '↓'
                parts.append(f'{col}{direction}')

            features_str = ', '.join(parts)
            return f'{features_str} ({variance:.0f}%)'

        x_axis_label = format_axis_label(pc1_contributors, 1, pc1_var)
        y_axis_label = format_axis_label(pc2_contributors, 2, pc2_var)

        # Categorical color palette for distinct groups
        CATEGORY_COLORS = [
            '#3498db',  # Blue
            '#e74c3c',  # Red
            '#2ecc71',  # Green
            '#9b59b6',  # Purple
            '#f39c12',  # Orange
            '#1abc9c',  # Teal
            '#e91e63',  # Pink
            '#795548',  # Brown
        ]

        traces = []
        show_legend = False
        target_info = None

        # Determine coloring strategy
        if target_column and df is not None and target_column in df.columns:
            # Color by target column - create separate trace per category for legend
            target_values = df.loc[row_indices, target_column].tolist()
            unique_values = sorted(set(str(v) for v in target_values if v is not None and str(v) != 'nan'))

            if len(unique_values) <= 8:
                # Categorical coloring with separate traces for legend
                show_legend = True
                target_info = {'column': target_column, 'values': unique_values}

                for i, val in enumerate(unique_values):
                    mask = [str(v) == val for v in target_values]
                    x_subset = [x for x, m in zip(x_data, mask) if m]
                    y_subset = [y for y, m in zip(y_data, mask) if m]

                    traces.append({
                        'type': 'scatter',
                        'x': x_subset,
                        'y': y_subset,
                        'mode': 'markers',
                        'name': f'{target_column}={val}',
                        'marker': {
                            'size': 8,
                            'color': CATEGORY_COLORS[i % len(CATEGORY_COLORS)],
                            'opacity': 0.7,
                            'line': {'width': 1, 'color': 'white'}
                        },
                        'hovertemplate': f'{target_column}: {val}<br>PC1: %{{x:.2f}}<br>PC2: %{{y:.2f}}<extra></extra>'
                    })
            else:
                # Too many categories - use continuous colorscale
                numeric_values = []
                for v in target_values:
                    try:
                        numeric_values.append(float(v))
                    except (ValueError, TypeError):
                        numeric_values.append(0)

                traces.append({
                    'type': 'scatter',
                    'x': x_data,
                    'y': y_data,
                    'mode': 'markers',
                    'marker': {
                        'size': 8,
                        'color': numeric_values,
                        'colorscale': 'Viridis',
                        'showscale': True,
                        'colorbar': {'title': target_column},
                        'opacity': 0.7,
                        'line': {'width': 1, 'color': 'white'}
                    },
                    'hovertemplate': f'{target_column}: %{{marker.color}}<br>PC1: %{{x:.2f}}<br>PC2: %{{y:.2f}}<extra></extra>'
                })
        else:
            # Color by outlier status if we have outliers, otherwise uniform color
            if any(outlier_mask):
                # Separate traces for outliers and normal points
                show_legend = True
                normal_x = [x for x, m in zip(x_data, outlier_mask) if not m]
                normal_y = [y for y, m in zip(y_data, outlier_mask) if not m]
                outlier_x = [x for x, m in zip(x_data, outlier_mask) if m]
                outlier_y = [y for y, m in zip(y_data, outlier_mask) if m]

                if normal_x:
                    traces.append({
                        'type': 'scatter',
                        'x': normal_x,
                        'y': normal_y,
                        'mode': 'markers',
                        'name': 'Normal',
                        'marker': {
                            'size': 8,
                            'color': '#3498db',
                            'opacity': 0.7,
                            'line': {'width': 1, 'color': 'white'}
                        },
                        'hovertemplate': 'Normal<br>PC1: %{x:.2f}<br>PC2: %{y:.2f}<extra></extra>'
                    })
                if outlier_x:
                    traces.append({
                        'type': 'scatter',
                        'x': outlier_x,
                        'y': outlier_y,
                        'mode': 'markers',
                        'name': 'Outlier',
                        'marker': {
                            'size': 8,
                            'color': '#e74c3c',
                            'opacity': 0.7,
                            'line': {'width': 1, 'color': 'white'}
                        },
                        'hovertemplate': 'Outlier<br>PC1: %{x:.2f}<br>PC2: %{y:.2f}<extra></extra>'
                    })
            else:
                # Uniform coloring
                traces.append({
                    'type': 'scatter',
                    'x': x_data,
                    'y': y_data,
                    'mode': 'markers',
                    'marker': {
                        'size': 8,
                        'color': '#3498db',
                        'opacity': 0.7,
                        'line': {'width': 1, 'color': 'white'}
                    },
                    'hovertemplate': 'PC1: %{x:.2f}<br>PC2: %{y:.2f}<extra></extra>'
                })

        layout = {
            'title': {
                'text': '2D Data Projection (PCA)',
                'font': {'size': 15, 'color': '#1e293b', 'family': 'system-ui, -apple-system, sans-serif'},
                'x': 0.5,
                'xanchor': 'center'
            },
            'xaxis': {
                'title': {
                    'text': x_axis_label,
                    'font': {'size': 12, 'color': '#475569'}
                },
                'gridcolor': 'rgba(226, 232, 240, 0.6)',
                'gridwidth': 1,
                'zeroline': True,
                'zerolinecolor': 'rgba(148, 163, 184, 0.5)',
                'zerolinewidth': 1,
                'tickfont': {'size': 11, 'color': '#64748b'},
                'showline': True,
                'linecolor': 'rgba(226, 232, 240, 0.8)',
                'linewidth': 1
            },
            'yaxis': {
                'title': {
                    'text': y_axis_label,
                    'font': {'size': 12, 'color': '#475569'}
                },
                'gridcolor': 'rgba(226, 232, 240, 0.6)',
                'gridwidth': 1,
                'zeroline': True,
                'zerolinecolor': 'rgba(148, 163, 184, 0.5)',
                'zerolinewidth': 1,
                'tickfont': {'size': 11, 'color': '#64748b'},
                'showline': True,
                'linecolor': 'rgba(226, 232, 240, 0.8)',
                'linewidth': 1
            },
            'plot_bgcolor': 'rgba(248, 250, 252, 0.5)',
            'paper_bgcolor': 'white',
            'hovermode': 'closest',
            'hoverlabel': {
                'bgcolor': 'white',
                'bordercolor': '#e2e8f0',
                'font': {'size': 12, 'color': '#1e293b', 'family': 'system-ui, -apple-system, sans-serif'}
            },
            'showlegend': show_legend,
            'legend': {
                'orientation': 'h',
                'yanchor': 'bottom',
                'y': 1.02,
                'xanchor': 'center',
                'x': 0.5,
                'bgcolor': 'rgba(255, 255, 255, 0.9)',
                'bordercolor': 'rgba(226, 232, 240, 0.5)',
                'borderwidth': 1,
                'font': {'size': 11, 'color': '#475569'}
            },
            'margin': {'l': 60, 'r': 30, 't': 60, 'b': 50}
        }

        return {
            'data': traces,
            'layout': layout,
            'config': {'responsive': True},
            'target_info': target_info  # Pass target info for interpretation
        }
