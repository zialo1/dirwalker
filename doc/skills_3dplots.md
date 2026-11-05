# 3D plot and interactive tree guidelines

Use these conventions when building or changing the interactive 3D directory tree in the HTML report.

## Layout

- Keep the plot responsive, with the canvas inside a bordered panel. The current panel is 90% wide, has an 8% left offset, and is 620 pixels high.
- Put the page count on the left above the panel and the first, previous, next, and last navigation buttons on the right, aligned to the panel edges.
- Put the sort labels and slider below the panel, aligned to its lower right edge. The sort slider must start at the leftmost option after a reload.
- Put the Copypaste button below the panel and its sort controls. Keep the “Hinweis:” label beside the button.
- Keep the table after the 3D panel with generous top spacing. Provide top-of-page links to the table and to the first activated group in the table.
- Do not show a symbol legend.

## Color and shape encoding

- Use one solid fill color per symbol. Do not split a symbol into differently colored regions.
- Assign each duplicate group a stable combination of color and shape based on its `groupId`; all directories in a group use the same combination.
- Use more colors than forms. The current palette has 14 colors and 13 forms, so color/shape combinations distinguish groups and repeat only after the combination cycle wraps.
- Current group colors, in order:

  `#1f77b4`, `#ff7f0e`, `#2ca02c`, `#d62728`, `#9467bd`, `#8c564b`, `#e377c2`, `#7f7f7f`, `#bcbd22`, `#17becf`, `#393b79`, `#637939`, `#8c6d31`, `#843c39`.

- Current forms, in order: circle, triangle, square, diamond, star, pentagon, cross, plus, heart, crescent, chevron, kite, and oval.
- Do not use six-sided, seven-sided, or nine-or-more-sided polygons.
- Keep shape selection independent of directory size. Directory size may affect marker radius and the list sorting, but not the group’s shape or color.
- A selected marker gets a conspicuous red outline while it remains in its group color. After Copypaste, its fill turns gray to show that it was copied.

## Navigation and interaction

- Support drag-to-rotate and wheel-to-zoom. Ignore clicks that are part of a drag.
- On hover over the root, show the full common path in large, bold text.
- On hover over a directory marker, show its seven-digit `dirid`, full path, and human-readable size.
- Clicking a directory marker in the final HTML toggles it in the copy buffer and gives it a conspicuous outline. Clicking it again before copying removes it from the buffer.
- The Copypaste button writes selected IDs grouped by group ID. After copying, show those markers in gray and mark their table IDs gray with a strike-through; append `*` to each affected table group ID.
- Make each starred group ID an anchor to its table row. The top link to the first activated group targets the first affected group ID.
- Disable marker selection and Copypaste in non-final, auto-refreshing reports.
