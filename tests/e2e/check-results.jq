# Unselected tests are normally reported as skipped. Selected tests must all pass.
(if $mode == "quick" then "Pods should be submitted and removed"
 elif $mode == "certified-conformance" then "[Conformance]"
 else error("unsupported conformance mode")
 end) as $focus
| [.[] | select(.name | contains($focus))] as $selected
| if length > 0
     and all(.[]; .status == "passed" or .status == "skipped")
     and ($selected | length > 0)
     and all($selected[]; .status == "passed")
  then {mode: $mode, passed: ($selected | length)}
  else error("missing, skipped, failed, or unknown selected e2e results")
  end
