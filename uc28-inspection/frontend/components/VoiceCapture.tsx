"use client";

import { useCallback, useEffect, useRef, useState } from "react";

interface Props {
  onTranscript: (text: string) => void;
}

export default function VoiceCapture({ onTranscript }: Props) {
  const [listening, setListening] = useState(false);
  const [interim, setInterim] = useState("");
  const recognitionRef = useRef<SpeechRecognition | null>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const animFrameRef = useRef<number>(0);
  const analyserRef = useRef<AnalyserNode | null>(null);

  const drawWaveform = useCallback(() => {
    const canvas = canvasRef.current;
    const analyser = analyserRef.current;
    if (!canvas || !analyser) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const data = new Uint8Array(analyser.frequencyBinCount);
    analyser.getByteFrequencyData(data);

    ctx.clearRect(0, 0, canvas.width, canvas.height);
    const barCount = 35;
    const barW = canvas.width / barCount - 2;

    for (let i = 0; i < barCount; i++) {
      const value = data[Math.floor((i / barCount) * data.length)];
      const barH = (value / 255) * canvas.height;
      ctx.fillStyle = i % 2 === 0 ? "#10B981" : "#6EE7B7";
      ctx.fillRect(i * (barW + 2), canvas.height - barH, barW, barH);
    }

    animFrameRef.current = requestAnimationFrame(drawWaveform);
  }, []);

  const startMic = useCallback(async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const ctx = new AudioContext();
      const source = ctx.createMediaStreamSource(stream);
      const analyser = ctx.createAnalyser();
      analyser.fftSize = 256;
      source.connect(analyser);
      analyserRef.current = analyser;
      animFrameRef.current = requestAnimationFrame(drawWaveform);
    } catch {
      // mic permission denied — waveform stays flat
    }
  }, [drawWaveform]);

  const stopMic = useCallback(() => {
    cancelAnimationFrame(animFrameRef.current);
    analyserRef.current = null;
    const canvas = canvasRef.current;
    if (canvas) {
      const ctx = canvas.getContext("2d");
      ctx?.clearRect(0, 0, canvas.width, canvas.height);
    }
  }, []);

  const toggleListening = useCallback(() => {
    const SpeechRecognition =
      (window as Window & typeof globalThis & { SpeechRecognition?: typeof window.SpeechRecognition; webkitSpeechRecognition?: typeof window.SpeechRecognition }).SpeechRecognition ||
      (window as Window & typeof globalThis & { webkitSpeechRecognition?: typeof window.SpeechRecognition }).webkitSpeechRecognition;

    if (!SpeechRecognition) {
      alert("Web Speech API non disponible. Utilisez la touche T pour saisir au clavier.");
      return;
    }

    if (listening) {
      recognitionRef.current?.stop();
      setListening(false);
      stopMic();
      return;
    }

    const recognition = new SpeechRecognition();
    recognition.lang = "fr-FR";
    recognition.continuous = true;
    recognition.interimResults = true;

    recognition.onresult = (e: SpeechRecognitionEvent) => {
      let finalText = "";
      let interimText = "";
      for (let i = e.resultIndex; i < e.results.length; i++) {
        if (e.results[i].isFinal) finalText += e.results[i][0].transcript;
        else interimText += e.results[i][0].transcript;
      }
      setInterim(interimText);
      if (finalText) {
        onTranscript(finalText.trim());
        setInterim("");
      }
    };

    recognition.onend = () => {
      setListening(false);
      stopMic();
    };

    recognitionRef.current = recognition;
    recognition.start();
    setListening(true);
    startMic();
  }, [listening, onTranscript, startMic, stopMic]);

  useEffect(() => {
    return () => {
      recognitionRef.current?.stop();
      cancelAnimationFrame(animFrameRef.current);
    };
  }, []);

  return (
    <div className="bg-uc-bg-dark rounded-lg p-4 flex flex-col gap-3">
      <div className="flex items-center gap-4">
        <button
          onClick={toggleListening}
          aria-label={listening ? "Arrêter l'enregistrement" : "Démarrer l'enregistrement"}
          className={`w-14 h-14 rounded-full flex items-center justify-center shrink-0 transition-colors ${
            listening ? "bg-uc-danger animate-pulse" : "bg-uc-text-mute"
          }`}
        >
          <span className={`w-4 h-4 rounded-full ${listening ? "bg-uc-alert" : "bg-white"}`} />
        </button>
        <canvas ref={canvasRef} width={300} height={56} className="flex-1 rounded" />
      </div>
      {interim && (
        <p className="text-sm italic text-uc-accent-lt">« {interim} »</p>
      )}
      {!listening && !interim && (
        <p className="text-xs text-uc-text-mute text-center">
          Cliquez le micro ou tapez <kbd className="bg-uc-bg-panel px-1 rounded">T</kbd> pour parler
        </p>
      )}
    </div>
  );
}
