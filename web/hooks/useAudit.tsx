'use client';

import { useState, useCallback, createContext, useContext, type ReactNode } from 'react';
import type { AuditMetadata } from '@/components/audit';

interface AuditContextType {
  audits: Map<string, AuditMetadata>;
  registerAudit: (key: string, audit: AuditMetadata) => void;
  getAudit: (key: string) => AuditMetadata | undefined;
  clearAudit: (key: string) => void;
  clearAllAudits: () => void;
  auditMode: boolean;
  toggleAuditMode: () => void;
}

const AuditContext = createContext<AuditContextType | null>(null);

export function AuditProvider({ children }: { children: ReactNode }) {
  const [audits, setAudits] = useState<Map<string, AuditMetadata>>(new Map());
  const [auditMode, setAuditMode] = useState(false);

  const registerAudit = useCallback((key: string, audit: AuditMetadata) => {
    setAudits((prev) => {
      const next = new Map(prev);
      next.set(key, audit);
      return next;
    });
  }, []);

  const getAudit = useCallback((key: string) => {
    return audits.get(key);
  }, [audits]);

  const clearAudit = useCallback((key: string) => {
    setAudits((prev) => {
      const next = new Map(prev);
      next.delete(key);
      return next;
    });
  }, []);

  const clearAllAudits = useCallback(() => {
    setAudits(new Map());
  }, []);

  const toggleAuditMode = useCallback(() => {
    setAuditMode((prev) => !prev);
  }, []);

  return (
    <AuditContext.Provider
      value={{
        audits,
        registerAudit,
        getAudit,
        clearAudit,
        clearAllAudits,
        auditMode,
        toggleAuditMode,
      }}
    >
      {children}
    </AuditContext.Provider>
  );
}

export function useAudit() {
  const context = useContext(AuditContext);
  if (!context) {
    throw new Error('useAudit must be used within an AuditProvider');
  }
  return context;
}

export function useAuditForKey(key: string) {
  const { getAudit, registerAudit, auditMode } = useAudit();

  const audit = getAudit(key);

  const setAudit = useCallback((auditData: AuditMetadata) => {
    registerAudit(key, auditData);
  }, [key, registerAudit]);

  return {
    audit,
    setAudit,
    showAuditButton: auditMode,
  };
}
