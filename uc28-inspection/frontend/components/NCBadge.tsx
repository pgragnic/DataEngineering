import type { NCLevel } from "@/lib/api";

const CONFIG: Record<NCLevel, { label: string; className: string }> = {
  nc_majeure:  { label: "NC MAJEURE",  className: "bg-uc-danger text-white" },
  nc_mineure:  { label: "NC MINEURE",  className: "bg-uc-alert text-white" },
  observation: { label: "OBSERVATION", className: "bg-uc-primary-2 text-white" },
  conforme:    { label: "CONFORME",    className: "bg-uc-accent text-white" },
};

export default function NCBadge({ level }: { level: NCLevel }) {
  const { label, className } = CONFIG[level];
  return (
    <span className={`inline-block px-2 py-0.5 text-xs font-bold tracking-wider rounded ${className}`}>
      {label}
    </span>
  );
}
