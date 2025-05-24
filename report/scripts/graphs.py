#!/usr/bin/env python3

import numpy as np

operations = [
  "Reads",
  "GetAttr",
  "Writes",
  "Open",
  "Close",
  "Create",
  "Readdir",
]

def acesses_graph():
    resnet_data = [
        39436,
        20069,
        55,
        72,
        72,
        4,
        2,
    ]
    cosmoflow_data = [
        5030,
        28217,
        189, #Almost all in the end, before the end it was 48
        5034,
        5034,
        7,
        2,
    ]
    import matplotlib.pyplot as plt
    # Horizontal bar graph
    y_pos = np.arange(len(operations))
    bar_width = 0.4

    plt.yticks(y_pos, operations)
    plt.xlabel('Number of Operations')
    plt.title('Number of FUSE Operations per Dataset')
    plt.xlim(0, 55000)
    plt.tight_layout()

    # Highlight groups (draw first, so bars are on top)
    highlight_alpha = 0.10
    ax = plt.gca()

    # Highlighted regions (no captions here)
    ax.axhspan(-0.5, 0.5, color='yellow', alpha=highlight_alpha, zorder=0)
    ax.axhspan(0.5, 1.5, color='red', alpha=highlight_alpha, zorder=0)
    ax.axhspan(1.5, 2.5, color='green', alpha=highlight_alpha, zorder=0)
    ax.axhspan(2.5, 4.5, color='cyan', alpha=highlight_alpha, zorder=0)
    ax.axhspan(4.5, 6.5, color='magenta', alpha=highlight_alpha, zorder=0)

    # Draw bars after highlights (higher zorder)
    bars1 = plt.barh(y_pos - bar_width/2, resnet_data, height=bar_width, color='blue', label='ResNet', zorder=2)
    bars2 = plt.barh(y_pos + bar_width/2, cosmoflow_data, height=bar_width, color='orange', label='CosmoFlow', zorder=2)

    plt.legend()
    # Add absolute frequency labels
    for bar in bars1:
        plt.text(bar.get_width() + 100, bar.get_y() + bar.get_height()/2, str(int(bar.get_width())), va='center', color='blue', zorder=3)
    for bar in bars2:
        plt.text(bar.get_width() + 100, bar.get_y() + bar.get_height()/2, str(int(bar.get_width())), va='center', color='orange', zorder=3)

    from matplotlib.patches import Patch

    # Bar handles (from your existing bars)
    bar_handles = [
        plt.Line2D([0], [0], color='blue', lw=6, label='ResNet'),
        plt.Line2D([0], [0], color='orange', lw=6, label='CosmoFlow')
    ]

    # Highlight region legend entries
    highlight_patches = [
        Patch(facecolor='yellow', alpha=0.18, label='Tied to file size'),
        Patch(facecolor='red', alpha=0.18, label='Tied to file size and file count'),
        Patch(facecolor='green', alpha=0.18, label='Tied to epoch count'),
        Patch(facecolor='cyan', alpha=0.18, label='Tied to file count'),
        Patch(facecolor='magenta', alpha=0.18, label='Fixed usage')
    ]

    # Combine all handles
    all_handles = bar_handles + highlight_patches

    # Place the combined legend
    plt.legend(handles=all_handles, loc='upper center', bbox_to_anchor=(0.75, 0.99),
            ncol=1, fontsize=9, frameon=True, title="Legend")

    plt.savefig('images/acesses_graph.svg', bbox_inches='tight')

if __name__ == "__main__":
    acesses_graph()
    print("Graph saved in images/acesses_graph.svg")
