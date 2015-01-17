(ns views.protocols)

(defprotocol IView
  (data [this namespace parameters]
    "Returns view data.")
  (relevant? [this namespace parameters hints]
    "Given hints of the form {:namespace x :hint y}, the view must
    return true if the hint indicates that an instance of this view
    with supplied namespace and parameters might require updating.
    It is always safe to return true, but false sure be returned only
    if you are sure this view does not need updating.")
  (id [this]
    "A unique identifer for a view."))

