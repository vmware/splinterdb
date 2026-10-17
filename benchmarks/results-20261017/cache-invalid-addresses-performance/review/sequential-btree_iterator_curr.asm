
.cache/invalid-address-perf-20261017/builds/sequential/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000408a0 <btree_iterator_curr>:
   408a0:	push   %r12
   408a2:	mov    %rsi,%r12
   408a5:	push   %rbp
   408a6:	mov    %rdx,%rbp
   408a9:	push   %rbx
   408aa:	mov    %rdi,%rbx
   408ad:	sub    $0x30,%rsp
   408b1:	cmpq   $0x0,0x70(%rdi)
   408b6:	je     408cd <btree_iterator_curr+0x2d>
   408b8:	mov    0x8(%rdi),%rdi
   408bc:	mov    0x68(%rbx),%rdx
   408c0:	lea    0x70(%rbx),%rsi
   408c4:	mov    (%rdi),%rax
   408c7:	call   *0xd0(%rax)
   408cd:	mov    0x80(%rbx),%rax
   408d4:	mov    0x1c(%rbx),%edx
   408d7:	cmpb   $0x0,0x20(%rax)
   408db:	jne    40940 <btree_iterator_curr+0xa0>
   408dd:	cmp    $0x2,%edx
   408e0:	je     40ae0 <btree_iterator_curr+0x240>
   408e6:	movzwl 0x90(%rbx),%edx
   408ed:	movzwl 0x28(%rax,%rdx,2),%ecx
   408f2:	add    %rax,%rcx
   408f5:	movzwl 0x26(%rax),%eax
   408f9:	movzwl (%rcx),%esi
   408fc:	test   %ax,%ax
   408ff:	jne    40a40 <btree_iterator_curr+0x1a0>
   40905:	lea    0x6(%rcx),%rdx
   40909:	mov    %rsi,%rax
   4090c:	mov    %rdx,%rdi
   4090f:	movq   $0x2,(%r12)
   40917:	add    %rsi,%rdx
   4091a:	movzwl 0x3(%rcx),%esi
   4091e:	mov    %rax,0x8(%r12)
   40923:	mov    %rdi,0x10(%r12)
   40928:	movzbl 0x5(%rcx),%eax
   4092c:	xor    %ecx,%ecx
   4092e:	test   $0x4,%al
   40930:	je     40936 <btree_iterator_curr+0x96>
   40932:	mov    0x8(%rbx),%rcx
   40936:	and    $0x3,%eax
   40939:	jmp    40a22 <btree_iterator_curr+0x182>
   4093e:	xchg   %ax,%ax
   40940:	cmp    $0x2,%edx
   40943:	je     40ab0 <btree_iterator_curr+0x210>
   40949:	movzwl 0x90(%rbx),%edx
   40950:	movzwl 0x28(%rax,%rdx,2),%edx
   40955:	add    %rdx,%rax
   40958:	movzwl 0x14(%rax),%edx
   4095c:	cmp    $0xffff,%dx
   40960:	je     40ac5 <btree_iterator_curr+0x225>
   40966:	cmp    $0xfffe,%dx
   4096a:	je     40af0 <btree_iterator_curr+0x250>
   40970:	lea    0x17(%rax),%rcx
   40974:	movzwl %dx,%esi
   40977:	mov    $0x2,%edi
   4097c:	mov    %rcx,0x10(%r12)
   40981:	pxor   %xmm3,%xmm3
   40985:	movq   (%rax),%xmm2
   40989:	mov    %edi,(%r12)
   4098d:	mov    0x10(%rax),%ecx
   40990:	movl   $0x0,0x4(%r12)
   40999:	mov    %rsi,0x8(%r12)
   4099e:	movq   0x8(%rax),%xmm0
   409a3:	punpckldq %xmm3,%xmm0
   409a7:	testb  $0x1,0x16(%rax)
   409ab:	je     40a60 <btree_iterator_curr+0x1c0>
   409b1:	add    $0x17,%rax
   409b5:	movzwl %dx,%esi
   409b8:	add    %rax,%rsi
   409bb:	cmp    $0xfffd,%dx
   409bf:	cmovbe %rsi,%rax
   409c3:	movq   (%rax),%xmm1
   409c7:	mov    0x8(%rax),%eax
   409ca:	shl    $0x20,%rax
   409ce:	movq   %xmm1,(%rsp)
   409d3:	punpckldq %xmm3,%xmm1
   409d7:	or     %rcx,%rax
   409da:	psllq  $0x20,%xmm1
   409df:	mov    %rax,0x20(%rsp)
   409e4:	por    %xmm0,%xmm1
   409e8:	movaps %xmm1,0x10(%rsp)
   409ed:	movdqa %xmm2,%xmm1
   409f1:	mov    $0x3e8,%eax
   409f6:	xor    %ecx,%ecx
   409f8:	mov    $0x20,%esi
   409fd:	movq   0x18(%rsp),%xmm0
   40a03:	movhps 0x10(%rsp),%xmm1
   40a08:	lea    0x138(%rbx),%rdx
   40a0f:	movups %xmm1,0x138(%rbx)
   40a16:	movhps 0x20(%rsp),%xmm0
   40a1b:	movups %xmm0,0x148(%rbx)
   40a22:	mov    %eax,0x0(%rbp)
   40a25:	mov    %rcx,0x8(%rbp)
   40a29:	mov    %rsi,0x10(%rbp)
   40a2d:	mov    %rdx,0x18(%rbp)
   40a31:	add    $0x30,%rsp
   40a35:	pop    %rbx
   40a36:	pop    %rbp
   40a37:	pop    %r12
   40a39:	ret
   40a3a:	nopw   0x0(%rax,%rax,1)
   40a40:	mov    0xc0(%rbx),%rdx
   40a47:	add    %rsi,%rax
   40a4a:	mov    0x50(%rdx),%rdi
   40a4e:	lea    0x6(%rcx),%rdx
   40a52:	jmp    4090f <btree_iterator_curr+0x6f>
   40a57:	nopw   0x0(%rax,%rax,1)
   40a60:	mov    $0x7fffffff,%eax
   40a65:	cmp    $0x7fffffff,%rcx
   40a6c:	movq   %rax,%xmm1
   40a71:	mov    $0xffffffffffffffff,%rax
   40a78:	punpcklqdq %xmm1,%xmm1
   40a7c:	cmove  %rax,%rcx
   40a80:	pcmpeqd %xmm0,%xmm1
   40a84:	mov    %rcx,0x20(%rsp)
   40a89:	pshufd $0xb1,%xmm1,%xmm3
   40a8e:	pand   %xmm3,%xmm1
   40a92:	movdqa %xmm1,%xmm3
   40a96:	pandn  %xmm0,%xmm3
   40a9a:	por    %xmm1,%xmm3
   40a9e:	movaps %xmm3,0x10(%rsp)
   40aa3:	jmp    409ed <btree_iterator_curr+0x14d>
   40aa8:	nopl   0x0(%rax,%rax,1)
   40ab0:	mov    0xb8(%rbx),%rax
   40ab7:	movzwl 0x14(%rax),%edx
   40abb:	cmp    $0xffff,%dx
   40abf:	jne    40966 <btree_iterator_curr+0xc6>
   40ac5:	xor    %ecx,%ecx
   40ac7:	mov    $0xffffffffffffffff,%rsi
   40ace:	mov    $0x1,%edi
   40ad3:	jmp    4097c <btree_iterator_curr+0xdc>
   40ad8:	nopl   0x0(%rax,%rax,1)
   40ae0:	mov    0xb8(%rbx),%rcx
   40ae7:	jmp    408f5 <btree_iterator_curr+0x55>
   40aec:	nopl   0x0(%rax)
   40af0:	xor    %ecx,%ecx
   40af2:	mov    $0xffffffffffffffff,%rsi
   40af9:	mov    $0x3,%edi
   40afe:	jmp    4097c <btree_iterator_curr+0xdc>

Disassembly of section .fini:
