
import { motion } from "framer-motion";
import type { RagChunk } from "@/lib/api";

interface Props {
  chunks: RagChunk[];
  loading?: boolean;
}

function ScoreChip({ score }: { score: number }) {
  const color =
    score >= 0.85
      ? "bg-uc-accent text-white"
      : score >= 0.7
      ? "bg-uc-alert text-white"
      : "bg-uc-text-mute text-white";
  return (
    <motion.span
      initial={{ scale: 0.8 }}
      animate={{ scale: 1 }}
      transition={{ duration: 0.15 }}
      className={`text-xs font-mono px-2 py-0.5 rounded-full ${color}`}
    >
      {score.toFixed(2)}
    </motion.span>
  );
}

export default function RagTransparency({ chunks, loading }: Props) {
  return (
    <div className="bg-uc-accent-50 border-t border-uc-accent px-6 py-3 shrink-0">
      <p className="text-center text-xs font-bold text-uc-primary mb-1">
        RAG · Articles normatifs remontés en temps réel
      </p>
      {loading ? (
        <div className="grid grid-cols-3 gap-4">
          {[0, 1, 2].map((i) => (
            <div key={i} className="bg-white rounded-lg p-3 animate-pulse h-20" />
          ))}
        </div>
      ) : chunks.length > 0 ? (
        <div className="grid grid-cols-3 gap-4">
          {chunks.map((chunk, i) => (
            <motion.div
              key={i}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: i * 0.1 }}
              className="bg-white rounded-lg p-3 flex flex-col gap-1"
            >
              <p className="text-xs font-bold text-uc-primary font-mono">
                §{chunk.section}
              </p>
              <p className="text-xs text-uc-text-body line-clamp-2 flex-1">
                {chunk.excerpt.slice(0, 120)}…
              </p>
              <div className="flex justify-end">
                <ScoreChip score={chunk.score} />
              </div>
            </motion.div>
          ))}
        </div>
      ) : (
        <p className="text-center text-xs italic text-uc-text-mute">
          Les chunks normatifs s&apos;affichent pendant la classification
        </p>
      )}
    </div>
  );
}
